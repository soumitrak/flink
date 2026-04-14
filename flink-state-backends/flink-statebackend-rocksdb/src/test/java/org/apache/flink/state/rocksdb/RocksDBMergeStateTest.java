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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingMergeState;
import org.apache.flink.api.common.state.AggregatingMergeStateDescriptor;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingMergeState;
import org.apache.flink.api.common.state.ReducingMergeStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocksDBReducingMergeState} and {@link RocksDBAggregatingMergeState}. */
class RocksDBMergeStateTest {

    @TempDir File tempDir;

    // -------------------------------------------------------------------------
    // ReducingMergeState tests
    // -------------------------------------------------------------------------

    @Test
    void testReducingMergeStateSum() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            assertThat(state.get()).isNull();

            state.add(10L);
            state.add(20L);
            state.add(30L);

            assertThat(state.get()).isEqualTo(60L);
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateMultipleKeys() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            ReducingMergeState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            backend.setCurrentKey(1);
            state.add(100L);
            state.add(200L);

            backend.setCurrentKey(2);
            state.add(5L);

            backend.setCurrentKey(1);
            assertThat(state.get()).isEqualTo(300L);

            backend.setCurrentKey(2);
            assertThat(state.get()).isEqualTo(5L);
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateStringConcat() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<String> desc =
                    new ReducingMergeStateDescriptor<>(
                            "concatState", (a, b) -> a + "," + b, StringSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add("hello");
            state.add("world");
            state.add("foo");

            assertThat(state.get()).isEqualTo("hello,world,foo");
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateClear() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add(42L);
            assertThat(state.get()).isEqualTo(42L);

            state.clear();
            assertThat(state.get()).isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateMergeNamespaces() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);

            // Populate source namespaces
            ReducingMergeState<Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(10L);
            stateNs1.add(20L);

            ReducingMergeState<Long> stateNs2 =
                    backend.getPartitionedState("ns2", StringSerializer.INSTANCE, desc);
            stateNs2.add(30L);

            // Merge ns1 and ns2 into target namespace "target"
            org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                            Integer, String, Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                                            Integer, String, Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1", "ns2"));

            ReducingMergeState<Long> targetState =
                    backend.getPartitionedState("target", StringSerializer.INSTANCE, desc);
            assertThat(targetState.get()).isEqualTo(60L);

            // Source namespaces should be deleted (re-acquire state to reset the current namespace)
            assertThat(backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc).get())
                    .isNull();
            assertThat(backend.getPartitionedState("ns2", StringSerializer.INSTANCE, desc).get())
                    .isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateMergeNamespacesIntoExistingTarget() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);

            ReducingMergeState<Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(10L);

            ReducingMergeState<Long> stateTarget =
                    backend.getPartitionedState("target", StringSerializer.INSTANCE, desc);
            stateTarget.add(100L);

            org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                            Integer, String, Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                                            Integer, String, Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1"));

            // target (100) + ns1 (10) = 110
            assertThat(backend.getPartitionedState("target", StringSerializer.INSTANCE, desc).get())
                    .isEqualTo(110L);
            assertThat(backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc).get())
                    .isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateMergeNamespacesWithNullAndMissingSource() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(42L);
            // ns2 intentionally has no data; null is passed as a third source

            org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                            Integer, String, Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalReducingMergeState<
                                            Integer, String, Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1", null, "ns2"));

            assertThat(backend.getPartitionedState("target", StringSerializer.INSTANCE, desc).get())
                    .isEqualTo(42L);
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // AggregatingMergeState tests
    // -------------------------------------------------------------------------

    @Test
    void testAggregatingMergeStateSum() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingMergeState<Long, long[], Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            assertThat(state.get()).isNull();

            state.add(10L);
            state.add(20L);
            state.add(30L);

            assertThat(state.get()).isEqualTo(20L);
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateMultipleKeys() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            AggregatingMergeState<Long, long[], Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            backend.setCurrentKey(1);
            state.add(100L);
            state.add(200L);

            backend.setCurrentKey(2);
            state.add(7L);

            backend.setCurrentKey(1);
            assertThat(state.get()).isEqualTo(150L);

            backend.setCurrentKey(2);
            assertThat(state.get()).isEqualTo(7L);
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateClear() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingMergeState<Long, long[], Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add(99L);
            assertThat(state.get()).isEqualTo(99L);

            state.clear();
            assertThat(state.get()).isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateMergeNamespaces() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);

            AggregatingMergeState<Long, long[], Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(10L);
            stateNs1.add(20L);

            AggregatingMergeState<Long, long[], Long> stateNs2 =
                    backend.getPartitionedState("ns2", StringSerializer.INSTANCE, desc);
            stateNs2.add(30L);

            org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                            Integer, String, Long, long[], Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                                            Integer, String, Long, long[], Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1", "ns2"));

            AggregatingMergeState<Long, long[], Long> targetState =
                    backend.getPartitionedState("target", StringSerializer.INSTANCE, desc);
            assertThat(targetState.get()).isEqualTo(20L);

            // Source namespaces should be deleted (re-acquire state to reset the current namespace)
            assertThat(backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc).get())
                    .isNull();
            assertThat(backend.getPartitionedState("ns2", StringSerializer.INSTANCE, desc).get())
                    .isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateMergeNamespacesIntoExistingTarget() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);

            AggregatingMergeState<Long, long[], Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(10L);
            stateNs1.add(20L); // ns1 acc: [30, 2]

            AggregatingMergeState<Long, long[], Long> stateTarget =
                    backend.getPartitionedState("target", StringSerializer.INSTANCE, desc);
            stateTarget.add(50L); // target acc: [50, 1]

            org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                            Integer, String, Long, long[], Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                                            Integer, String, Long, long[], Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1"));

            // merged acc: [30+50, 2+1] = [80, 3] → average = 26
            assertThat(backend.getPartitionedState("target", StringSerializer.INSTANCE, desc).get())
                    .isEqualTo(26L);
            assertThat(backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc).get())
                    .isNull();
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateMergeNamespacesWithNullAndMissingSource() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggSum",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingMergeState<Long, long[], Long> stateNs1 =
                    backend.getPartitionedState("ns1", StringSerializer.INSTANCE, desc);
            stateNs1.add(60L);
            stateNs1.add(90L); // ns1 acc: [150, 2] → avg = 75
            // ns2 intentionally has no data; null is passed as a third source

            org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                            Integer, String, Long, long[], Long>
                    internalState =
                            (org.apache.flink.runtime.state.internal.InternalAggregatingMergeState<
                                            Integer, String, Long, long[], Long>)
                                    stateNs1;
            internalState.mergeNamespaces("target", Arrays.asList("ns1", null, "ns2"));

            assertThat(backend.getPartitionedState("target", StringSerializer.INSTANCE, desc).get())
                    .isEqualTo(75L);
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // set() tests for ReducingMergeState
    // -------------------------------------------------------------------------

    @Test
    void testReducingMergeStateSet() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add(10L);
            state.add(20L);
            assertThat(state.get()).isEqualTo(30L);

            // set() must overwrite the previously merged value
            state.set(100L);
            assertThat(state.get()).isEqualTo(100L);
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testReducingMergeStateSetThenAdd() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            ReducingMergeStateDescriptor<Long> desc =
                    new ReducingMergeStateDescriptor<>(
                            "sumState", Long::sum, LongSerializer.INSTANCE);

            backend.setCurrentKey(1);
            ReducingMergeState<Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add(10L);
            state.add(20L);
            // reset to 100, then add more on top
            state.set(100L);
            state.add(5L);
            assertThat(state.get()).isEqualTo(105L);
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // set() tests for AggregatingMergeState
    // -------------------------------------------------------------------------

    @Test
    void testAggregatingMergeStateSet() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggAvg",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingMergeState<Long, long[], Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            state.add(10L);
            state.add(20L);
            assertThat(state.get()).isEqualTo(15L); // (10+20)/2

            // set() must overwrite the previously merged accumulator
            state.set(new long[] {100L, 1L}); // acc = [sum=100, count=1]
            assertThat(state.get()).isEqualTo(100L); // 100/1
        } finally {
            backend.dispose();
        }
    }

    @Test
    void testAggregatingMergeStateSetThenAdd() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            AggregatingMergeStateDescriptor<Long, long[], Long> desc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggAvg",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            backend.setCurrentKey(1);
            AggregatingMergeState<Long, long[], Long> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            // seed the accumulator, then add more values on top
            state.set(new long[] {100L, 1L}); // acc = [sum=100, count=1]
            state.add(50L); // acc = [sum=150, count=2]
            assertThat(state.get()).isEqualTo(75L); // 150/2
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // Mixed (old + new merge operators coexisting in the same backend)
    // -------------------------------------------------------------------------

    /**
     * Registers all four state types in the same RocksDB backend and verifies that the standard
     * (stringappendtest) merge operator used by {@link RocksDBReducingState} and {@link
     * RocksDBAggregatingState} does not interfere with the custom Java merge operators used by
     * {@link RocksDBReducingMergeState} and {@link RocksDBAggregatingMergeState}, and vice versa.
     *
     * <p>Each state gets its own column family with its own merge operator:
     *
     * <ul>
     *   <li>{@code reducingState} — column family "reducingState", stringappendtest operator,
     *       read-modify-write add()
     *   <li>{@code aggregatingState} — column family "aggregatingState", stringappendtest operator,
     *       read-modify-write add()
     *   <li>{@code reducingMergeState} — column family "reducingMergeState",
     *       RocksDBReducingMergeOperator, write-only add()
     *   <li>{@code aggregatingMergeState} — column family "aggregatingMergeState",
     *       RocksDBAggregatingMergeOperator, write-only add()
     * </ul>
     */
    @Test
    void testAllFourStateTypesCoexistInSameBackend() throws Exception {
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            // --- Descriptors ---
            ReducingStateDescriptor<Long> reducingDesc =
                    new ReducingStateDescriptor<>(
                            "reducingState", Long::sum, LongSerializer.INSTANCE);

            AggregatingStateDescriptor<Long, long[], Long> aggregatingDesc =
                    new AggregatingStateDescriptor<>(
                            "aggregatingState",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            ReducingMergeStateDescriptor<Long> reducingMergeDesc =
                    new ReducingMergeStateDescriptor<>(
                            "reducingMergeState", Long::sum, LongSerializer.INSTANCE);

            AggregatingMergeStateDescriptor<Long, long[], Long> aggregatingMergeDesc =
                    new AggregatingMergeStateDescriptor<>(
                            "aggregatingMergeState",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);

            // --- Key 1: interleave writes to all four state types ---
            backend.setCurrentKey(1);

            ReducingState<Long> reducingState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc);
            AggregatingState<Long, Long> aggregatingState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            aggregatingDesc);
            ReducingMergeState<Long> reducingMergeState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            reducingMergeDesc);
            AggregatingMergeState<Long, long[], Long> aggregatingMergeState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            aggregatingMergeDesc);

            reducingState.add(10L);
            reducingMergeState.add(10L);
            aggregatingState.add(10L);
            aggregatingMergeState.add(10L);

            reducingState.add(20L);
            reducingMergeState.add(20L);
            aggregatingState.add(20L);
            aggregatingMergeState.add(20L);

            reducingState.add(30L);
            reducingMergeState.add(30L);
            aggregatingState.add(30L);
            aggregatingMergeState.add(30L);

            // Reducing states sum; aggregating states average
            assertThat(reducingState.get()).isEqualTo(60L);
            assertThat(aggregatingState.get()).isEqualTo(20L);
            assertThat(reducingMergeState.get()).isEqualTo(60L);
            assertThat(aggregatingMergeState.get()).isEqualTo(20L);

            // --- Key 2: verify state isolation between keys ---
            backend.setCurrentKey(2);

            reducingState.add(1L);
            reducingMergeState.add(1L);
            aggregatingState.add(1L);
            aggregatingMergeState.add(1L);

            assertThat(reducingState.get()).isEqualTo(1L);
            assertThat(aggregatingState.get()).isEqualTo(1L);
            assertThat(reducingMergeState.get()).isEqualTo(1L);
            assertThat(aggregatingMergeState.get()).isEqualTo(1L);

            // Key 1 values must be unaffected by key 2 writes
            backend.setCurrentKey(1);
            assertThat(reducingState.get()).isEqualTo(60L);
            assertThat(aggregatingState.get()).isEqualTo(20L);
            assertThat(reducingMergeState.get()).isEqualTo(60L);
            assertThat(aggregatingMergeState.get()).isEqualTo(20L);

            // --- Clear one merge state, verify the others are unaffected ---
            reducingMergeState.clear();
            assertThat(reducingMergeState.get()).isNull();
            // The other three must still hold their values
            assertThat(reducingState.get()).isEqualTo(60L);
            assertThat(aggregatingState.get()).isEqualTo(20L);
            assertThat(aggregatingMergeState.get()).isEqualTo(20L);
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // Performance tests
    // -------------------------------------------------------------------------

    /**
     * Measures and prints the time taken by {@link ReducingState}, {@link ReducingMergeState},
     * {@link AggregatingState}, and {@link AggregatingMergeState} to each add 100,000 values.
     *
     * <p>ReducingState and AggregatingState perform a synchronous read-modify-write on every {@code
     * add()}, while the merge-state variants issue a write-only RocksDB merge operand and defer the
     * actual aggregation to compaction / read time.
     */
    @Test
    void testPerformanceComparison() throws Exception {
        final int count = 1_000_000;

        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(tempDir, IntSerializer.INSTANCE).build();

        try {
            backend.setCurrentKey(1);

            // --- ReducingState ---
            ReducingStateDescriptor<Long> reducingDesc =
                    new ReducingStateDescriptor<>(
                            "perf_reducing", Long::sum, LongSerializer.INSTANCE);
            ReducingState<Long> reducingState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc);

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                reducingState.add((long) i);
            }
            long reducingAddMs = (System.nanoTime() - start) / 1_000_000;

            start = System.nanoTime();
            reducingState.get();
            long reducingGetMs = (System.nanoTime() - start) / 1_000_000;

            // --- ReducingMergeState ---
            ReducingMergeStateDescriptor<Long> reducingMergeDesc =
                    new ReducingMergeStateDescriptor<>(
                            "perf_reducing_merge", Long::sum, LongSerializer.INSTANCE);
            ReducingMergeState<Long> reducingMergeState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            reducingMergeDesc);

            start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                reducingMergeState.add((long) i);
            }
            long reducingMergeAddMs = (System.nanoTime() - start) / 1_000_000;

            start = System.nanoTime();
            reducingMergeState.get();
            long reducingMergeGetMs = (System.nanoTime() - start) / 1_000_000;

            // --- AggregatingState ---
            AggregatingStateDescriptor<Long, long[], Long> aggDesc =
                    new AggregatingStateDescriptor<>(
                            "perf_aggregating",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);
            AggregatingState<Long, Long> aggState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc);

            start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                aggState.add((long) i);
            }
            long aggAddMs = (System.nanoTime() - start) / 1_000_000;

            start = System.nanoTime();
            aggState.get();
            long aggGetMs = (System.nanoTime() - start) / 1_000_000;

            // --- AggregatingMergeState ---
            AggregatingMergeStateDescriptor<Long, long[], Long> aggMergeDesc =
                    new AggregatingMergeStateDescriptor<>(
                            "perf_aggregating_merge",
                            new AverageAggregateFunction(),
                            LongPrimitiveArraySerializer.INSTANCE);
            AggregatingMergeState<Long, long[], Long> aggMergeState =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggMergeDesc);

            start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                aggMergeState.add((long) i);
            }
            long aggMergeAddMs = (System.nanoTime() - start) / 1_000_000;

            start = System.nanoTime();
            aggMergeState.get();
            long aggMergeGetMs = (System.nanoTime() - start) / 1_000_000;

            System.out.printf(
                    "%-30s add(%,d): %,d ms  get: %,d ms%n",
                    "ReducingState", count, reducingAddMs, reducingGetMs);
            System.out.printf(
                    "%-30s add(%,d): %,d ms  get: %,d ms%n",
                    "ReducingMergeState", count, reducingMergeAddMs, reducingMergeGetMs);
            System.out.printf(
                    "%-30s add(%,d): %,d ms  get: %,d ms%n",
                    "AggregatingState", count, aggAddMs, aggGetMs);
            System.out.printf(
                    "%-30s add(%,d): %,d ms  get: %,d ms%n",
                    "AggregatingMergeState", count, aggMergeAddMs, aggMergeGetMs);
        } finally {
            backend.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // Checkpoint / Savepoint restore tests
    // -------------------------------------------------------------------------

    /**
     * Full (non-incremental) checkpoint → restore cycle for both merge-state types.
     *
     * <p>Verifies that (a) the restored values are correct, (b) the {@code mergeFunctionBytes} are
     * present in the restored meta-info (proving the function was serialised into the snapshot),
     * and (c) further {@code add()} calls after restore still work correctly.
     */
    @Test
    void testFullCheckpointAndRestore() throws Exception {
        File checkpointDir = new File(tempDir, "checkpoint");
        checkpointDir.mkdirs();

        ReducingMergeStateDescriptor<Long> reducingDesc =
                new ReducingMergeStateDescriptor<>("reduceSum", Long::sum, LongSerializer.INSTANCE);
        AggregatingMergeStateDescriptor<Long, long[], Long> aggDesc =
                new AggregatingMergeStateDescriptor<>(
                        "aggAvg",
                        new AverageAggregateFunction(),
                        LongPrimitiveArraySerializer.INSTANCE);

        KeyedStateHandle handle;
        File backendDir1 = new File(tempDir, "backend1");
        backendDir1.mkdirs();
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(backendDir1, IntSerializer.INSTANCE)
                        .build();
        try {
            backend.setCurrentKey(1);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(10L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(20L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(100L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(200L);

            var streamFactory =
                    new JobManagerCheckpointStorage()
                            .createCheckpointStorage(new JobID())
                            .resolveCheckpointStorageLocation(
                                    1L, CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            1L,
                            1L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotFuture.run();
            handle = snapshotFuture.get().getJobManagerOwnedSnapshot();
            handle.registerSharedStates(new SharedStateRegistryImpl(), 1L);
        } finally {
            backend.dispose();
        }

        // Restore
        File backendDir2 = new File(tempDir, "backend2");
        backendDir2.mkdirs();
        RocksDBKeyedStateBackend<Integer> restored =
                RocksDBTestUtils.builderForTestDefaults(
                                backendDir2,
                                IntSerializer.INSTANCE,
                                2,
                                new KeyGroupRange(0, 1),
                                Collections.singletonList(handle))
                        .build();
        try {
            restored.setCurrentKey(1);

            // Verify restored values
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(30L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            aggDesc)
                                    .get())
                    .isEqualTo(150L);

            // Verify merge function bytes are present in the restored meta-info
            assertMergeFunctionBytesPresent(restored, reducingDesc.getName());
            assertMergeFunctionBytesPresent(restored, aggDesc.getName());

            // Verify add() still works after restore
            restored.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(30L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(60L);
        } finally {
            restored.dispose();
        }
    }

    /**
     * Savepoint → restore cycle for both merge-state types.
     *
     * <p>Savepoints use a different code path (full re-snapshot without SST file reuse) and a
     * dedicated {@link SavepointType}. This test verifies that the function bytes survive a
     * canonical savepoint round-trip.
     */
    @Test
    void testSavepointAndRestore() throws Exception {
        File checkpointDir = new File(tempDir, "savepoint");
        checkpointDir.mkdirs();

        ReducingMergeStateDescriptor<Long> reducingDesc =
                new ReducingMergeStateDescriptor<>("reduceSum", Long::sum, LongSerializer.INSTANCE);
        AggregatingMergeStateDescriptor<Long, long[], Long> aggDesc =
                new AggregatingMergeStateDescriptor<>(
                        "aggAvg",
                        new AverageAggregateFunction(),
                        LongPrimitiveArraySerializer.INSTANCE);

        KeyedStateHandle handle;
        File backendDir1 = new File(tempDir, "sp-backend1");
        backendDir1.mkdirs();
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(backendDir1, IntSerializer.INSTANCE)
                        .build();
        try {
            backend.setCurrentKey(1);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(5L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(15L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(40L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(60L);

            var streamFactory =
                    new FileSystemCheckpointStorage(checkpointDir.toURI())
                            .createCheckpointStorage(new JobID())
                            .resolveCheckpointStorageLocation(
                                    1L, CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            1L,
                            1L,
                            streamFactory,
                            CheckpointOptions.alignedNoTimeout(
                                    SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                    CheckpointStorageLocationReference.getDefault()));
            snapshotFuture.run();
            handle = snapshotFuture.get().getJobManagerOwnedSnapshot();
            handle.registerSharedStates(new SharedStateRegistryImpl(), 1L);
        } finally {
            backend.dispose();
        }

        // Restore
        File backendDir2 = new File(tempDir, "sp-backend2");
        backendDir2.mkdirs();
        RocksDBKeyedStateBackend<Integer> restored =
                RocksDBTestUtils.builderForTestDefaults(
                                backendDir2,
                                IntSerializer.INSTANCE,
                                2,
                                new KeyGroupRange(0, 1),
                                Collections.singletonList(handle))
                        .build();
        try {
            restored.setCurrentKey(1);

            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(20L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            aggDesc)
                                    .get())
                    .isEqualTo(50L);

            assertMergeFunctionBytesPresent(restored, reducingDesc.getName());
            assertMergeFunctionBytesPresent(restored, aggDesc.getName());

            // add() must still work after savepoint restore
            restored.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(10L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(30L);
        } finally {
            restored.dispose();
        }
    }

    /**
     * Incremental checkpoint → restore cycle for both merge-state types.
     *
     * <p>Incremental checkpoints reuse SST files across checkpoints. When the backend is restored,
     * RocksDB opens the existing SST files and must apply any pending merge operands using the
     * correct custom merge operator. This test verifies that the merge operator is reconstructed
     * correctly so that pending operands are processed right.
     */
    @Test
    void testIncrementalCheckpointAndRestore() throws Exception {
        File checkpointDir = new File(tempDir, "inc-checkpoint");
        checkpointDir.mkdirs();

        ReducingMergeStateDescriptor<Long> reducingDesc =
                new ReducingMergeStateDescriptor<>("reduceSum", Long::sum, LongSerializer.INSTANCE);
        AggregatingMergeStateDescriptor<Long, long[], Long> aggDesc =
                new AggregatingMergeStateDescriptor<>(
                        "aggAvg",
                        new AverageAggregateFunction(),
                        LongPrimitiveArraySerializer.INSTANCE);

        KeyedStateHandle handle;
        File backendDir1 = new File(tempDir, "inc-backend1");
        backendDir1.mkdirs();
        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(
                                backendDir1,
                                IntSerializer.INSTANCE,
                                2,
                                new KeyGroupRange(0, 1),
                                Collections.emptyList())
                        .setEnableIncrementalCheckpointing(true)
                        .build();
        try {
            var streamFactory =
                    new FileSystemCheckpointStorage(checkpointDir.toURI())
                            .createCheckpointStorage(new JobID())
                            .resolveCheckpointStorageLocation(
                                    1L, CheckpointStorageLocationReference.getDefault());

            backend.setCurrentKey(1);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(7L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(3L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(10L);
            backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, aggDesc)
                    .add(30L);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            1L,
                            1L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotFuture.run();
            SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
            handle = snapshotResult.getJobManagerOwnedSnapshot();
            handle.registerSharedStates(new SharedStateRegistryImpl(), 1L);
            backend.notifyCheckpointComplete(1L);
        } finally {
            backend.dispose();
        }

        // Restore with incremental checkpointing enabled
        File backendDir2 = new File(tempDir, "inc-backend2");
        backendDir2.mkdirs();
        RocksDBKeyedStateBackend<Integer> restored =
                RocksDBTestUtils.builderForTestDefaults(
                                backendDir2,
                                IntSerializer.INSTANCE,
                                2,
                                new KeyGroupRange(0, 1),
                                Collections.singletonList(handle))
                        .setEnableIncrementalCheckpointing(true)
                        .build();
        try {
            restored.setCurrentKey(1);

            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(10L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            aggDesc)
                                    .get())
                    .isEqualTo(20L);

            assertMergeFunctionBytesPresent(restored, reducingDesc.getName());
            assertMergeFunctionBytesPresent(restored, aggDesc.getName());

            // add() must still work after incremental restore
            restored.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, reducingDesc)
                    .add(10L);
            assertThat(
                            restored.getPartitionedState(
                                            VoidNamespace.INSTANCE,
                                            VoidNamespaceSerializer.INSTANCE,
                                            reducingDesc)
                                    .get())
                    .isEqualTo(20L);
        } finally {
            restored.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Asserts that the named state in the backend has {@code mergeFunctionBytes} stored in its
     * meta-info, confirming the function was serialised into the checkpoint/savepoint.
     */
    private static void assertMergeFunctionBytesPresent(
            RocksDBKeyedStateBackend<?> backend, String stateName) {
        RocksDbKvStateInfo stateInfo = backend.getKvStateInformation().get(stateName);
        assertThat(stateInfo).as("State info must exist for: " + stateName).isNotNull();
        RegisteredStateMetaInfoBase metaInfoBase = stateInfo.metaInfo;
        assertThat(metaInfoBase)
                .as("Meta info must be RegisteredKeyValueStateBackendMetaInfo")
                .isInstanceOf(RegisteredKeyValueStateBackendMetaInfo.class);
        RegisteredKeyValueStateBackendMetaInfo<?, ?> kvMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<?, ?>) metaInfoBase;
        assertThat(kvMetaInfo.getMergeFunctionBytes())
                .as("mergeFunctionBytes must be non-null for state: " + stateName)
                .isNotNull();
    }

    /** Average aggregate function: IN = Long, ACC = long[]{sum, count}, OUT = Long. */
    private static class AverageAggregateFunction implements AggregateFunction<Long, long[], Long> {

        @Override
        public long[] createAccumulator() {
            return new long[] {0L, 0L};
        }

        @Override
        public long[] add(Long value, long[] accumulator) {
            return new long[] {accumulator[0] + value, accumulator[1] + 1};
        }

        @Override
        public Long getResult(long[] accumulator) {
            return accumulator[0] / accumulator[1];
        }

        @Override
        public long[] merge(long[] a, long[] b) {
            return new long[] {a[0] + b[0], a[1] + b[1]};
        }
    }
}
