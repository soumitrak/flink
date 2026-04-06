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
     * Folds a single operand into the existing value using the reduce function.
     *
     * <p>If no existing value is present the operand is returned as-is. Otherwise the existing
     * value and the operand are combined via {@link ReduceFunction#reduce}.
     *
     * @param key the RocksDB key (unused)
     * @param existing the current base value (serialized {@code V}), or {@code null} if absent
     * @param value the merge operand (serialized {@code V})
     * @return the serialized reduced value
     */
    @Override
    public byte[] merge(ByteBuffer key, ByteBuffer existing, ByteBuffer value) {
        try {
            DataInputDeserializer input = new DataInputDeserializer();
            TypeSerializer<V> ser = valueSerializer.duplicate();

            V valueDeserialized = deserialize(value, ser, input);
            if (existing == null) {
                return serialize(valueDeserialized, ser);
            }
            V existingDeserialized = deserialize(existing, ser, input);
            return serialize(reduceFunction.reduce(existingDeserialized, valueDeserialized), ser);
        } catch (Exception e) {
            throw new RuntimeException("Error in RocksDBReducingMergeOperator.merge", e);
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
