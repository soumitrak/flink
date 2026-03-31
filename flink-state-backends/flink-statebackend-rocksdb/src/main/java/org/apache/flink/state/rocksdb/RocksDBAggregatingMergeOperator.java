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

import org.rocksdb.AbstractMergeOperator;

import java.nio.ByteBuffer;

/**
 * A RocksDB {@link AbstractMergeOperator} that applies an {@link AggregateFunction} to merge state
 * operands. Used by {@link RocksDBAggregatingMergeState} to avoid synchronous reads on {@code
 * add()}.
 *
 * <p>The stored base value is a serialized {@code ACC} (accumulator). Each merge operand is a
 * serialized {@code IN} (input value). On {@code fullMerge}, each input operand is added to the
 * accumulator via {@link AggregateFunction#add}. Partial merge is declined (returns {@code null})
 * because input and accumulator types may differ.
 *
 * <p>Thread safety: fresh serializer instances are created per call.
 *
 * @param <IN> The type of values added to the state
 * @param <ACC> The accumulator type stored in RocksDB
 * @param <OUT> The result type returned from {@code get()}
 */
class RocksDBAggregatingMergeOperator<IN, ACC, OUT> extends AbstractMergeOperator {

    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final TypeSerializer<IN> inputSerializer;
    private final TypeSerializer<ACC> accSerializer;

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

    @Override
    public byte[] fullMerge(ByteBuffer key, ByteBuffer existing, ByteBuffer[] operands) {
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

            for (ByteBuffer operand : operands) {
                IN value = deserializeIn(operand, inSer, input);
                acc = aggFunction.add(value, acc);
            }

            return serializeAcc(acc, accSer);
        } catch (Exception e) {
            throw new RuntimeException("Error in RocksDBAggregatingMergeOperator.fullMerge", e);
        }
    }

    // Partial merge is declined because IN and ACC may be different types.
    // RocksDB will keep both operands and let fullMerge handle them.
    @Override
    public byte[] partialMerge(ByteBuffer key, ByteBuffer left, ByteBuffer right) {
        return null;
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
