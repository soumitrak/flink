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
 * A {@link ReducingState} variant backed by a RocksDB merge operator.
 *
 * <p>Unlike the standard {@link ReducingState}, whose {@code add()} performs a synchronous
 * read-modify-write, this state uses {@code db.merge()} to append operands without reading the
 * existing value. The reduce function is applied lazily by RocksDB during compaction or on {@code
 * get()}.
 *
 * <p>This state type is only supported by the RocksDB state backend. Using it with the heap-based
 * backend will throw {@link UnsupportedOperationException} at state registration time.
 *
 * @param <T> Type of the value in the state
 */
@PublicEvolving
public interface ReducingMergeState<T> extends ReducingState<T> {}
