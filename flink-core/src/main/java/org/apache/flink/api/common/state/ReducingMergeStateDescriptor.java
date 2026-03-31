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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link StateDescriptor} for {@link ReducingMergeState}. This can be used to create a RocksDB
 * merge-operator-backed reducing state via {@link
 * org.apache.flink.api.common.functions.RuntimeContext#getReducingMergeState(ReducingMergeStateDescriptor)}.
 *
 * <p>This state type is only supported by the RocksDB state backend.
 *
 * @param <T> The type of the values in the state.
 */
@PublicEvolving
public class ReducingMergeStateDescriptor<T> extends StateDescriptor<ReducingMergeState<T>, T> {

    private static final long serialVersionUID = 1L;

    private final ReduceFunction<T> reduceFunction;

    /**
     * Creates a new {@code ReducingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to reduce the state.
     * @param typeClass The type of the values in the state.
     */
    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, Class<T> typeClass) {
        super(name, typeClass, null);
        this.reduceFunction = checkNotNull(reduceFunction);
        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException(
                    "ReduceFunction of ReducingMergeState cannot be a RichFunction.");
        }
    }

    /**
     * Creates a new {@code ReducingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to reduce the state.
     * @param typeInfo The type of the values in the state.
     */
    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, TypeInformation<T> typeInfo) {
        super(name, typeInfo, null);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /**
     * Creates a new {@code ReducingMergeStateDescriptor}.
     *
     * @param name The (unique) name for the state.
     * @param reduceFunction The {@code ReduceFunction} used to reduce the state.
     * @param typeSerializer The serializer for the values in the state.
     */
    public ReducingMergeStateDescriptor(
            String name, ReduceFunction<T> reduceFunction, TypeSerializer<T> typeSerializer) {
        super(name, typeSerializer, null);
        this.reduceFunction = checkNotNull(reduceFunction);
    }

    /** Returns the reduce function. */
    public ReduceFunction<T> getReduceFunction() {
        return reduceFunction;
    }

    @Override
    public Type getType() {
        return Type.REDUCING_MERGE;
    }
}
