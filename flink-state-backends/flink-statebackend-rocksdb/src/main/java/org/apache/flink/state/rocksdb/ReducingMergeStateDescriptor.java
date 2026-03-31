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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * RocksDB-specific subclass of {@link
 * org.apache.flink.api.common.state.ReducingMergeStateDescriptor} that additionally exposes {@link
 * #createMergeOperator(TypeSerializer)} for backend-internal use.
 *
 * @deprecated Use {@link org.apache.flink.api.common.state.ReducingMergeStateDescriptor} directly.
 * @param <T> The type of the values in the state.
 */
@Deprecated
@PublicEvolving
public class ReducingMergeStateDescriptor<T>
        extends org.apache.flink.api.common.state.ReducingMergeStateDescriptor<T> {

    private static final long serialVersionUID = 1L;

    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, Class<T> typeClass) {
        super(name, reduceFunction, typeClass);
    }

    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, TypeInformation<T> typeInfo) {
        super(name, reduceFunction, typeInfo);
    }

    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, TypeSerializer<T> typeSerializer) {
        super(name, reduceFunction, typeSerializer);
    }

    /**
     * Creates a {@link RocksDBReducingMergeOperator} for this descriptor.
     *
     * @deprecated Construct {@link RocksDBReducingMergeOperator} directly from {@link
     *     #getReduceFunction()} and the serializer.
     */
    @Deprecated
    public RocksDBReducingMergeOperator<T> createMergeOperator(TypeSerializer<T> serializer) {
        return new RocksDBReducingMergeOperator<>(getReduceFunction(), serializer);
    }
}
