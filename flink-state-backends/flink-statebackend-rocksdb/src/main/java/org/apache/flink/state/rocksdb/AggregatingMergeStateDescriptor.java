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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * RocksDB-specific subclass of {@link
 * org.apache.flink.api.common.state.AggregatingMergeStateDescriptor} that additionally exposes
 * {@link #createMergeOperator(TypeSerializer)} for backend-internal use.
 *
 * @deprecated Use {@link org.apache.flink.api.common.state.AggregatingMergeStateDescriptor}
 *     directly.
 * @param <IN> The type of values added to the state.
 * @param <ACC> The accumulator type stored in RocksDB.
 * @param <OUT> The result type returned from {@code get()}.
 */
@Deprecated
@PublicEvolving
public class AggregatingMergeStateDescriptor<IN, ACC, OUT>
        extends org.apache.flink.api.common.state.AggregatingMergeStateDescriptor<IN, ACC, OUT> {

    private static final long serialVersionUID = 1L;

    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            Class<ACC> accType) {
        super(name, aggFunction, inputSerializer, accType);
    }

    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            TypeInformation<ACC> accTypeInfo) {
        super(name, aggFunction, inputSerializer, accTypeInfo);
    }

    public AggregatingMergeStateDescriptor(
            String name,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<ACC> accSerializer) {
        super(name, aggFunction, inputSerializer, accSerializer);
    }

    /**
     * Creates a {@link RocksDBAggregatingMergeOperator} for this descriptor.
     *
     * @deprecated Construct {@link RocksDBAggregatingMergeOperator} directly from {@link
     *     #getAggregateFunction()}, {@link #getInputSerializer()}, and the accumulator serializer.
     */
    @Deprecated
    public RocksDBAggregatingMergeOperator<IN, ACC, OUT> createMergeOperator(
            TypeSerializer<ACC> accSerializer) {
        return new RocksDBAggregatingMergeOperator<>(
                getAggregateFunction(), getInputSerializer(), accSerializer);
    }
}
