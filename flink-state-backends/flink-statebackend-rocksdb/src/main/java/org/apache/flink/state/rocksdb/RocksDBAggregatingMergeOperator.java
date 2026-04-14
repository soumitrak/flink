/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.rocksdb.AbstractAssociativeMergeOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A RocksDB {@link AbstractAssociativeMergeOperator} that applies an {@link AggregateFunction} to
 * merge state operands. Used by {@link RocksDBAggregatingMergeState} to avoid synchronous reads on
 * {@code add()}.
 *
 * <p>Both the stored base value and each merge operand are serialized {@code ACC} (accumulator)
 * values. {@link RocksDBAggregatingMergeState#add} converts each {@code IN} input into an
 * {@code ACC} via {@link AggregateFunction#add} before writing the merge operand, so the
 * operator always handles {@code ACC} on both sides. On each {@code merge} call the two
 * accumulators are combined via {@link AggregateFunction#merge}. If no existing value is
 * present, a fresh accumulator is created via {@link AggregateFunction#createAccumulator()}
 * and the operand is merged into it.
 *
 * <p>Thread safety: fresh serializer instances are created per call.
 *
 * @param <IN> The type of values added to the state
 * @param <ACC> The accumulator type stored in RocksDB
 * @param <OUT> The result type returned from {@code get()}
 */
class RocksDBAggregatingMergeOperator<IN, ACC, OUT> extends AbstractAssociativeMergeOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBAggregatingMergeOperator.class);

    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final TypeSerializer<ACC> accSerializer;

    /**
     * Class loader captured from the user thread at construction time. RocksDB invokes {@link
     * #merge} from a native compaction thread that is attached to the JVM with no context class
     * loader set (null). Serializers backed by Kryo call {@code
     * Thread.currentThread().getContextClassLoader()} during lazy initialization and fail with
     * {@code IllegalArgumentException: classLoader cannot be null}. We restore this class loader on
     * the compaction thread before any (de)serialization and clean it up in a finally block.
     */
    private final ClassLoader userClassLoader;

    /**
     * Creates a new operator with the given aggregate function and serializers. Captures the
     * context class loader of the calling thread as the user class loader.
     *
     * @param aggFunction the aggregate function to apply during merge
     * @param accSerializer serializer for accumulator values ({@code ACC})
     */
    RocksDBAggregatingMergeOperator(
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<ACC> accSerializer) {
        this(aggFunction, accSerializer, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Creates a new operator with an explicit user class loader. Use this overload when the
     * calling thread's context class loader may not be the user code class loader — e.g., during
     * restore from checkpoint/savepoint where the class loader is supplied explicitly.
     *
     * @param aggFunction the aggregate function to apply during merge
     * @param accSerializer serializer for accumulator values ({@code ACC})
     * @param userClassLoader the class loader to set on RocksDB compaction threads before
     *     (de)serialization
     */
    RocksDBAggregatingMergeOperator(
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<ACC> accSerializer,
            ClassLoader userClassLoader) {
        super();
        this.aggFunction = aggFunction;
        this.accSerializer = accSerializer;
        this.userClassLoader = userClassLoader;
    }

    @Override
    public String name() {
        return "RocksDBAggregatingMergeOperator";
    }

    /**
     * Applies the aggregate function to fold a single operand into the existing accumulator.
     *
     * <p>If no existing value is present a fresh accumulator is created via {@link
     * AggregateFunction#createAccumulator()}. The operand (serialized {@code ACC}) is deserialized
     * and merged into the accumulator via {@link AggregateFunction#merge}. The resulting
     * accumulator is serialized and written into {@code output}.
     *
     * @param key the RocksDB key (unused)
     * @param existing the current base value (serialized {@code ACC}), or {@code null} if absent
     * @param value the merge operand (serialized {@code ACC})
     * @param output writable direct ByteBuffer to receive the merged result (position=0 on entry)
     * @return the number of bytes written to {@code output}, or {@code -1} on failure
     */
    @Override
    public int merge(ByteBuffer key, ByteBuffer existing, ByteBuffer value, ByteBuffer output) {
        // Restore the user class loader on this (native compaction) thread so that serializers
        // backed by Kryo can initialize without hitting "classLoader cannot be null".
        Thread thread = Thread.currentThread();
        ClassLoader savedClassLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(userClassLoader);
        try {
            DataInputDeserializer input = new DataInputDeserializer();
            // duplicate() produces an independent serializer instance, making this method
            // safe to call concurrently from multiple compaction threads.
            TypeSerializer<ACC> accSer = accSerializer.duplicate();

            ACC acc;
            if (existing != null) {
                acc = deserializeAcc(existing, accSer, input);
            } else {
                acc = aggFunction.createAccumulator();
            }

            ACC accValue = deserializeAcc(value, accSer, input);
            acc = aggFunction.merge(accValue, acc);

            byte[] resultBytes = serializeAcc(acc, accSer);
            output.put(resultBytes);
            return resultBytes.length;
        } catch (Exception e) {
            // Log before returning -1: the JNI bridge treats -1 as a merge failure, causing a
            // RocksDB background error that surfaces as Corruption(Undefined) at checkpoint time.
            // Without logging here the failure would be silent.
            LOG.error(
                    "RocksDBAggregatingMergeOperator.merge failed — RocksDB will set a background "
                            + "error and subsequent checkpoints will fail with Corruption(Undefined)",
                    e);
            return -1;
        } finally {
            thread.setContextClassLoader(savedClassLoader);
        }
    }

    private ACC deserializeAcc(ByteBuffer buf, TypeSerializer<ACC> ser, DataInputDeserializer input)
            throws Exception {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        input.setBuffer(bytes);
        return ser.deserialize(input);
    }

    private byte[] serializeAcc(ACC acc, TypeSerializer<ACC> ser) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        ser.serialize(acc, output);
        return output.getCopyOfBuffer();
    }
}
