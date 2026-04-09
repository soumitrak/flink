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
import org.apache.flink.api.common.state.ReducingMergeState;
import org.apache.flink.api.common.state.ReducingMergeStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalReducingMergeState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link ReducingMergeState} implementation that stores state in RocksDB using the merge operator
 * for write-only {@code add()} operations.
 *
 * <p>Instead of reading the existing value, applying the reduce function, and writing back
 * (read-modify-write), {@code add()} calls {@code db.merge()} which buffers the operand. The {@link
 * RocksDBReducingMergeOperator} applies the reduce function during compaction or on {@code get()}.
 *
 * @param <K> The type of the key
 * @param <N> The type of the namespace
 * @param <V> The type of the value
 */
class RocksDBReducingMergeState<K, N, V> extends AbstractRocksDBAppendingState<K, N, V, V, V>
        implements InternalReducingMergeState<K, N, V> {

    private ReduceFunction<V> reduceFunction;

    private RocksDBReducingMergeState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            ReduceFunction<V> reduceFunction,
            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
        this.reduceFunction = reduceFunction;
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
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    /**
     * Returns the reduced value for the current key and namespace. Reading from RocksDB will
     * trigger a full merge of any pending operands via {@link RocksDBReducingMergeOperator}.
     *
     * @return the reduced value, or {@code null} if no value has been added yet
     */
    @Override
    public V get() throws IOException, RocksDBException {
        return getInternal();
    }

    /**
     * Overwrites the current state with the given value using {@code db.put()}, discarding any
     * previously merged operands. Subsequent {@code add()} calls will be reduced on top of this
     * value.
     */
    @Override
    public void set(V value) throws RocksDBException {
        updateInternal(value);
    }

    /**
     * Appends the value as a RocksDB merge operand. The reduce function is applied lazily by the
     * {@link RocksDBReducingMergeOperator} during compaction or on {@code get()}.
     */
    @Override
    public void add(V value) throws IOException, RocksDBException {
        backend.db.merge(
                columnFamily,
                writeOptions,
                serializeCurrentKeyWithGroupAndNamespace(),
                serializeValue(value));
    }

    /**
     * Merges the values from all {@code sources} namespaces into the {@code target} namespace. Each
     * source value is read, deleted, and folded into a running result using the reduce function.
     * The result is then reduced with any existing target value and written back.
     */
    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        V current = null;

        for (N source : sources) {
            if (source != null) {
                setCurrentNamespace(source);
                final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
                final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);

                if (valueBytes != null) {
                    backend.db.delete(columnFamily, writeOptions, sourceKey);
                    dataInputView.setBuffer(valueBytes);
                    V value = valueSerializer.deserialize(dataInputView);
                    current = (current == null) ? value : reduceFunction.reduce(current, value);
                }
            }
        }

        if (current != null) {
            setCurrentNamespace(target);
            final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
            final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

            if (targetValueBytes != null) {
                dataInputView.setBuffer(targetValueBytes);
                V value = valueSerializer.deserialize(dataInputView);
                current = reduceFunction.reduce(current, value);
            }

            dataOutputView.clear();
            valueSerializer.serialize(current, dataOutputView);
            backend.db.put(columnFamily, writeOptions, targetKey, dataOutputView.getCopyOfBuffer());
        }
    }

    /** Updates the reduce function; called on state re-registration. */
    RocksDBReducingMergeState<K, N, V> setReduceFunction(ReduceFunction<V> reduceFunction) {
        this.reduceFunction = reduceFunction;
        return this;
    }

    /**
     * Factory method called by the RocksDB state backend to create a new instance backed by the
     * provided column family.
     */
    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBReducingMergeState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        ((ReducingMergeStateDescriptor<SV>) stateDesc).getReduceFunction(),
                        backend);
    }

    /**
     * Update method called by the RocksDB state backend when an existing state instance is
     * re-registered. Updates the reduce function, namespace serializer, value serializer, default
     * value, and column family handle on the existing object in-place.
     */
    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBReducingMergeState<K, N, SV>) existingState)
                        .setReduceFunction(
                                ((ReducingMergeStateDescriptor<SV>) stateDesc).getReduceFunction())
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue())
                        .setColumnFamily(registerResult.f0);
    }
}
