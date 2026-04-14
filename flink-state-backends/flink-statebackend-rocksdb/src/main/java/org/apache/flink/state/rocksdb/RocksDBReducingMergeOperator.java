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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.rocksdb.AbstractAssociativeMergeOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A RocksDB {@link AbstractAssociativeMergeOperator} that applies a {@link ReduceFunction} to merge
 * state operands. Used by {@link RocksDBReducingMergeState} to avoid synchronous reads on {@code
 * add()}.
 *
 * <p>The stored value and each operand are serialized {@code V} values. On each {@code merge} call
 * a single operand is folded into the existing value via the reduce function. RocksDB automatically
 * chains multiple operands and handles partial merging during compaction.
 *
 * <p>Thread safety: fresh serializer instances are created per call, so this class is thread-safe
 * for concurrent invocations from compaction threads and user threads.
 *
 * @param <V> The type of the state value
 */
class RocksDBReducingMergeOperator<V> extends AbstractAssociativeMergeOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBReducingMergeOperator.class);

    private final ReduceFunction<V> reduceFunction;
    private final TypeSerializer<V> valueSerializer;

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
     * Creates a new operator with the given reduce function and value serializer. Captures the
     * context class loader of the calling thread as the user class loader.
     *
     * @param reduceFunction the reduce function applied during merge
     * @param valueSerializer serializer for state values ({@code V})
     */
    RocksDBReducingMergeOperator(
            ReduceFunction<V> reduceFunction, TypeSerializer<V> valueSerializer) {
        this(reduceFunction, valueSerializer, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Creates a new operator with an explicit user class loader. Use this overload when the
     * calling thread's context class loader may not be the user code class loader — e.g., during
     * restore from checkpoint/savepoint where the class loader is supplied explicitly.
     *
     * @param reduceFunction the reduce function applied during merge
     * @param valueSerializer serializer for state values ({@code V})
     * @param userClassLoader the class loader to set on RocksDB compaction threads before
     *     (de)serialization
     */
    RocksDBReducingMergeOperator(
            ReduceFunction<V> reduceFunction,
            TypeSerializer<V> valueSerializer,
            ClassLoader userClassLoader) {
        super();
        this.reduceFunction = reduceFunction;
        this.valueSerializer = valueSerializer;
        this.userClassLoader = userClassLoader;
    }

    @Override
    public String name() {
        return "RocksDBReducingMergeOperator";
    }

    /**
     * Folds a single operand into the existing value using the reduce function.
     *
     * <p>If no existing value is present the operand is written to {@code output} as-is.
     * Otherwise the existing value and the operand are combined via {@link ReduceFunction#reduce}
     * and the result is written into {@code output}.
     *
     * @param key the RocksDB key (unused)
     * @param existing the current base value (serialized {@code V}), or {@code null} if absent
     * @param value the merge operand (serialized {@code V})
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
            TypeSerializer<V> ser = valueSerializer.duplicate();

            V valueDeserialized = deserialize(value, ser, input);
            byte[] resultBytes;
            if (existing == null) {
                resultBytes = serialize(valueDeserialized, ser);
            } else {
                V existingDeserialized = deserialize(existing, ser, input);
                resultBytes =
                        serialize(reduceFunction.reduce(existingDeserialized, valueDeserialized), ser);
            }
            output.put(resultBytes);
            return resultBytes.length;
        } catch (Exception e) {
            // Log before returning -1: the JNI bridge treats -1 as a merge failure, causing a
            // RocksDB background error that surfaces as Corruption(Undefined) at checkpoint time.
            // Without logging here the failure would be silent.
            LOG.error(
                    "RocksDBReducingMergeOperator.merge failed — RocksDB will set a background "
                            + "error and subsequent checkpoints will fail with Corruption(Undefined)",
                    e);
            return -1;
        } finally {
            thread.setContextClassLoader(savedClassLoader);
        }
    }

    private V deserialize(ByteBuffer buf, TypeSerializer<V> ser, DataInputDeserializer input)
            throws Exception {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        input.setBuffer(bytes);
        return ser.deserialize(input);
    }

    private byte[] serialize(V value, TypeSerializer<V> ser) throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(64);
        ser.serialize(value, output);
        return output.getCopyOfBuffer();
    }
}
