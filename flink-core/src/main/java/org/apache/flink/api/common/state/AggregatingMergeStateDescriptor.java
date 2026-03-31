/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StateDescriptor} for {@link AggregatingMergeState}. This can be used to create a RocksDB
 * merge-operator-backed aggregating state via {@link
 * org.apache.flink.api.common.functions.RuntimeContext#getAggregatingMergeState(AggregatingMergeStateDescriptor)}.
 *
 * <p>The type internally stored in the state is the type of the {@code Accumulator} of the {@code
 * AggregateFunction}.
 *
 * <p>This state type is only supported by the RocksDB state backend.
 *
 * @param <IN> The type of values added to the state.
 * @param <ACC> The accumulator type stored in RocksDB.
 * @param <OUT> The result type returned from {@code get()}.
 */
@PublicEvolving
public class AggregatingMergeStateDescriptor<IN, ACC, OUT>
        extends StateDescriptor<AggregatingMergeState<IN, OUT>, ACC> {

    private static final long serialVersionUID = 1L;

    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final TypeSerializer<IN> inputSerializer;

    /**
     * Creates a new {@code AggregatingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param inputSerializer Serializer for input values ({@code IN}).
     * @param accType The type of the accumulator stored in the state.
     */
    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            Class<ACC> accType) {
        super(name, accType, null);
        this.aggFunction = checkNotNull(aggFunction);
        this.inputSerializer = checkNotNull(inputSerializer);
    }

    /**
     * Creates a new {@code AggregatingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param inputSerializer Serializer for input values ({@code IN}).
     * @param accTypeInfo Type information for the accumulator.
     */
    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            TypeInformation<ACC> accTypeInfo) {
        super(name, accTypeInfo, null);
        this.aggFunction = checkNotNull(aggFunction);
        this.inputSerializer = checkNotNull(inputSerializer);
    }

    /**
     * Creates a new {@code AggregatingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param aggFunction The {@code AggregateFunction} used to aggregate the state.
     * @param inputSerializer Serializer for input values ({@code IN}).
     * @param accSerializer Serializer for the accumulator.
     */
    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<ACC> accSerializer) {
        super(name, accSerializer, null);
        this.aggFunction = checkNotNull(aggFunction);
        this.inputSerializer = checkNotNull(inputSerializer);
    }

    /** Returns the aggregate function. */
    public AggregateFunction<IN, ACC, OUT> getAggregateFunction() {
        return aggFunction;
    }

    /** Returns the serializer for input values ({@code IN}). */
    public TypeSerializer<IN> getInputSerializer() {
        return inputSerializer;
    }

    @Override
    public Type getType() {
        return Type.AGGREGATING_MERGE;
    }
}
