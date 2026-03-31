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
import org.apache.flink.api.common.state.AggregatingMergeState;
import org.apache.flink.api.common.state.AggregatingMergeStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalAggregatingMergeState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collection;

/**
 * An {@link AggregatingMergeState} implementation that stores state in RocksDB using the merge
 * operator for write-only {@code add()} operations.
 *
 * <p>The stored base value is a serialized {@code ACC}. Each {@code add(IN)} call serializes the
 * input as {@code IN} and calls {@code db.merge()}, without reading the accumulator. The {@link
 * RocksDBAggregatingMergeOperator} applies the aggregate function during compaction or on {@code
 * get()}.
 *
 * @param <K> The type of the key
 * @param <N> The type of the namespace
 * @param <IN> The type of input values added to the state
 * @param <ACC> The accumulator type stored in RocksDB
 * @param <R> The result type returned from {@code get()}
 */
class RocksDBAggregatingMergeState<K, N, IN, ACC, R>
        extends AbstractRocksDBAppendingState<K, N, IN, ACC, R>
        implements InternalAggregatingMergeState<K, N, IN, ACC, R> {

    private AggregateFunction<IN, ACC, R> aggFunction;
    private TypeSerializer<IN> inputSerializer;

    private RocksDBAggregatingMergeState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> accSerializer,
            ACC defaultValue,
            AggregateFunction<IN, ACC, R> aggFunction,
            TypeSerializer<IN> inputSerializer,
            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, accSerializer, defaultValue, backend);
        this.aggFunction = aggFunction;
        this.inputSerializer = inputSerializer;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<ACC> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public R get() throws IOException, RocksDBException {
        ACC accumulator = getInternal();
        if (accumulator == null) {
            return null;
        }
        return aggFunction.getResult(accumulator);
    }

    /**
     * Serializes the input value as {@code IN} and appends it as a RocksDB merge operand. The
     * aggregate function is applied lazily by the {@link RocksDBAggregatingMergeOperator} during
     * compaction or on {@code get()}.
     */
    @Override
    public void add(IN value) throws IOException, RocksDBException {
        dataOutputView.clear();
        inputSerializer.serialize(value, dataOutputView);
        backend.db.merge(
                columnFamily,
                writeOptions,
                serializeCurrentKeyWithGroupAndNamespace(),
                dataOutputView.getCopyOfBuffer());
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources)
            throws IOException, RocksDBException {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        ACC current = null;

        for (N source : sources) {
            if (source != null) {
                setCurrentNamespace(source);
                final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
                final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);

                if (valueBytes != null) {
                    backend.db.delete(columnFamily, writeOptions, sourceKey);
                    dataInputView.setBuffer(valueBytes);
                    ACC value = valueSerializer.deserialize(dataInputView);
                    current = (current == null) ? value : aggFunction.merge(current, value);
                }
            }
        }

        if (current != null) {
            setCurrentNamespace(target);
            final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
            final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

            if (targetValueBytes != null) {
                dataInputView.setBuffer(targetValueBytes);
                ACC value = valueSerializer.deserialize(dataInputView);
                current = aggFunction.merge(current, value);
            }

            dataOutputView.clear();
            valueSerializer.serialize(current, dataOutputView);
            backend.db.put(columnFamily, writeOptions, targetKey, dataOutputView.getCopyOfBuffer());
        }
    }

    RocksDBAggregatingMergeState<K, N, IN, ACC, R> setAggFunction(
            AggregateFunction<IN, ACC, R> aggFunction) {
        this.aggFunction = aggFunction;
        return this;
    }

    RocksDBAggregatingMergeState<K, N, IN, ACC, R> setInputSerializer(
            TypeSerializer<IN> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        AggregatingMergeStateDescriptor desc = (AggregatingMergeStateDescriptor) stateDesc;
        return (IS)
                new RocksDBAggregatingMergeState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        desc.getAggregateFunction(),
                        desc.getInputSerializer(),
                        backend);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        AggregatingMergeStateDescriptor desc = (AggregatingMergeStateDescriptor) stateDesc;
        return (IS)
                ((RocksDBAggregatingMergeState<K, N, ?, SV, ?>) existingState)
                        .setAggFunction(desc.getAggregateFunction())
                        .setInputSerializer(desc.getInputSerializer())
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue())
                        .setColumnFamily(registerResult.f0);
    }
}
