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

import java.nio.ByteBuffer;

/**
 * A RocksDB {@link AbstractAssociativeMergeOperator} that applies an {@link AggregateFunction} to
 * merge state operands. Used by {@link RocksDBAggregatingMergeState} to avoid synchronous reads on
 * {@code add()}.
 *
 * <p>The stored base value is a serialized {@code ACC} (accumulator). Each merge operand is a
 * serialized {@code IN} (input value). On each {@code merge} call, the operand is added to the
 * accumulator via {@link AggregateFunction#add}. If no existing value is present, a fresh
 * accumulator is created via {@link AggregateFunction#createAccumulator()}.
 *
 * <p>Thread safety: fresh serializer instances are created per call.
 *
 * @param <IN> The type of values added to the state
 * @param <ACC> The accumulator type stored in RocksDB
 * @param <OUT> The result type returned from {@code get()}
 */
class RocksDBAggregatingMergeOperator<IN, ACC, OUT> extends AbstractAssociativeMergeOperator {

    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final TypeSerializer<IN> inputSerializer;
    private final TypeSerializer<ACC> accSerializer;

    /**
     * Creates a new operator with the given aggregate function and serializers.
     *
     * @param aggFunction the aggregate function to apply during merge
     * @param inputSerializer serializer for input values ({@code IN})
     * @param accSerializer serializer for accumulator values ({@code ACC})
     */
    RocksDBAggregatingMergeOperator(
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<ACC> accSerializer) {
        super();
        this.aggFunction = aggFunction;
        this.inputSerializer = inputSerializer;
        this.accSerializer = accSerializer;
    }

    @Override
    public String name() {
        return "RocksDBAggregatingMergeOperator";
    }

    /**
     * Applies the aggregate function to fold a single operand into the existing accumulator.
     *
     * <p>If no existing value is present a fresh accumulator is created via {@link
     * AggregateFunction#createAccumulator()}. The operand (serialized {@code IN}) is deserialized
     * and added to the accumulator via {@link AggregateFunction#add}. The resulting accumulator is
     * serialized and returned as the new base value.
     *
     * @param key the RocksDB key (unused)
     * @param existing the current base value (serialized {@code ACC}), or {@code null} if absent
     * @param value the merge operand (serialized {@code IN})
     * @return the serialized merged accumulator
     */
    @Override
    public byte[] merge(ByteBuffer key, ByteBuffer existing, ByteBuffer value) {
        try {
            DataInputDeserializer input = new DataInputDeserializer();
            TypeSerializer<IN> inSer = inputSerializer.duplicate();
            TypeSerializer<ACC> accSer = accSerializer.duplicate();

            ACC acc;
            if (existing != null) {
                acc = deserializeAcc(existing, accSer, input);
            } else {
                acc = aggFunction.createAccumulator();
            }

            IN inValue = deserializeIn(value, inSer, input);
            acc = aggFunction.add(inValue, acc);

            return serializeAcc(acc, accSer);
        } catch (Exception e) {
            throw new RuntimeException("Error in RocksDBAggregatingMergeOperator.merge", e);
        }
    }

    private IN deserializeIn(ByteBuffer buf, TypeSerializer<IN> ser, DataInputDeserializer input)
            throws Exception {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        input.setBuffer(bytes);
        return ser.deserialize(input);
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
