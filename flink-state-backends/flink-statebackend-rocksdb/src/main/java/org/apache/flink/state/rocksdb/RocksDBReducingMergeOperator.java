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

import org.rocksdb.AbstractMergeOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A RocksDB {@link AbstractMergeOperator} that applies a {@link ReduceFunction} to merge state
 * operands. Used by {@link RocksDBReducingMergeState} to avoid synchronous reads on {@code add()}.
 *
 * <p>The stored value and each operand are serialized {@code V} values. On {@code fullMerge} they
 * are folded left-to-right via the reduce function. On {@code partialMerge} two adjacent operands
 * are pre-reduced.
 *
 * <p>Thread safety: fresh serializer instances are created per call, so this class is thread-safe
 * for concurrent invocations from compaction threads and user threads.
 *
 * @param <V> The type of the state value
 */
class RocksDBReducingMergeOperator<V> extends AbstractMergeOperator {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBReducingMergeOperator.class);

    private final ReduceFunction<V> reduceFunction;
    private final TypeSerializer<V> valueSerializer;

    /**
     * Creates a new operator with the given reduce function and value serializer.
     *
     * @param reduceFunction the reduce function applied during merge
     * @param valueSerializer serializer for state values ({@code V})
     */
    RocksDBReducingMergeOperator(
            ReduceFunction<V> reduceFunction, TypeSerializer<V> valueSerializer) {
        super();
        this.reduceFunction = reduceFunction;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public String name() {
        return "RocksDBReducingMergeOperator";
    }

    /**
     * Folds the existing base value and all pending operands into a single value using the reduce
     * function.
     *
     * <p>Values are processed left-to-right: the existing base (if present) is taken as the initial
     * accumulator, then each operand is reduced into it. If neither a base value nor any operands
     * are present, {@code null} is returned.
     *
     * @param key the RocksDB key (unused)
     * @param existing the current base value (serialized {@code V}), or {@code null} if absent
     * @param operands the pending merge operands (each a serialized {@code V})
     * @return the serialized reduced value, or {@code null} if there is nothing to reduce
     */
    @Override
    public byte[] fullMerge(ByteBuffer key, ByteBuffer existing, ByteBuffer[] operands) {
        try {
            DataInputDeserializer input = new DataInputDeserializer();
            TypeSerializer<V> ser = valueSerializer.duplicate();

            V current = null;
            if (existing != null) {
                current = deserialize(existing, ser, input);
            }

            for (ByteBuffer operand : operands) {
                V value = deserialize(operand, ser, input);
                current = (current == null) ? value : reduceFunction.reduce(current, value);
            }

            if (current == null) {
                return null;
            }
            return serialize(current, ser);
        } catch (Exception e) {
            throw new RuntimeException("Error in RocksDBReducingMergeOperator.fullMerge", e);
        }
    }

    /**
     * Pre-reduces two adjacent operands during compaction. This allows RocksDB to compact the merge
     * operand list incrementally without waiting for a full merge. On failure, returns {@code null}
     * to defer reduction to the next {@link #fullMerge} call.
     *
     * @param key the RocksDB key (unused)
     * @param left the left operand (serialized {@code V})
     * @param right the right operand (serialized {@code V})
     * @return the serialized reduced value, or {@code null} if partial merge fails
     */
    @Override
    public byte[] partialMerge(ByteBuffer key, ByteBuffer left, ByteBuffer right) {
        try {
            DataInputDeserializer input = new DataInputDeserializer();
            TypeSerializer<V> ser = valueSerializer.duplicate();

            V l = deserialize(left, ser, input);
            V r = deserialize(right, ser, input);
            return serialize(reduceFunction.reduce(l, r), ser);
        } catch (Exception e) {
            // Decline partial merge on error; fullMerge will handle it later
            return null;
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
