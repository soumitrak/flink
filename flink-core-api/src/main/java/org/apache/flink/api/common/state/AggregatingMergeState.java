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

/**
 * An {@link AggregatingState} variant backed by a RocksDB merge operator.
 *
 * <p>Unlike the standard {@link AggregatingState}, whose {@code add()} performs a synchronous
 * read-modify-write, this state uses {@code db.merge()} to append input values without reading the
 * accumulator. The aggregate function is applied lazily by RocksDB during compaction or on {@code
 * get()}.
 *
 * <p>This state type is only supported by the RocksDB state backend. Using it with the heap-based
 * backend will throw {@link UnsupportedOperationException} at state registration time.
 *
 * @param <IN> Type of the value added to the state
 * @param <ACC> Type of the accumulator stored in the state
 * @param <OUT> Type of the value returned from the state
 */
@PublicEvolving
public interface AggregatingMergeState<IN, ACC, OUT> extends AggregatingState<IN, OUT> {

    /**
     * Overwrites the current state with the given value, discarding any previously merged
     * operands. Subsequent {@code add()} calls will be aggregated on top of this value.
     * The implementation must create the accumulator with this value and store the
     * accumulator.
     *
     * @param value the value to set as the new state
     */
    void set(IN value) throws Exception;

    /**
     * Overwrites the current state with the given accumulator, discarding any previously merged
     * operands. Subsequent {@code add()} calls will be aggregated on top of this accumulator.
     *
     * @param acc the accumulator value to set as the new state
     */
    void setAcc(ACC acc) throws Exception;
}
