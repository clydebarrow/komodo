/*
 *
 *  * Copyright 2018 Control-J Pty Ltd
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  *
 *
 */

package com.controlj.komodo

import io.reactivex.Flowable
import org.h2.mvstore.MVMap
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 13:19
 *
 * Delete a set of items from a map via a query.
 * @param map   The KoMap to be used
 * @param index The index to be used
 * @param lowerBound    The lower bound KeyValue. No default
 * @param upperBound    The upper bound KeyValue. No default
 * @param start    A start position - this many values will be skipped before any are emitted
 * @param limit The maximum number of values to be emitted
 * @param reverse If set, the values will be emitted in reverse order, i.e. starting with the upperBound
 */
class Delete<V> internal constructor(
        private val map: KoMap<V>,
        private val index: MVMap<ByteArray, ByteArray>,
        private val lowerBound: KeyWrapper,
        private val upperBound: KeyWrapper,
        private val start: Int,
        private val limit: Int,
        private val reverse: Boolean
) : Flowable<V>() {


    override fun subscribeActual(s: Subscriber<in V>) {
        val firstKey: ByteArray?
        val lastKey: ByteArray?
        val upperKey = when (upperBound) {
            KeyWrapper.END -> index.lastKey()
            KeyWrapper.START -> index.firstKey()
            else -> {
                val last = index.ceilingKey(upperBound.byteArray)
                if (upperBound.isPrefixOf(last)) last else index.lowerKey(last)
            }
        }
        val lowerKey = when (lowerBound) {
            KeyWrapper.END -> index.lastKey()
            KeyWrapper.START -> index.firstKey()
            else -> index.ceilingKey(lowerBound.byteArray)
        }
        if (reverse) {
            firstKey = upperKey
            lastKey = lowerKey
        } else {
            firstKey = lowerKey
            lastKey = upperKey
        }

        s.onSubscribe(object : Subscription {
            var cancelled = false
            var nextKey = firstKey
            var position = 0

            override fun cancel() {
                cancelled = true
            }

            override fun request(n: Long) {
                var count = n
                while (count != 0L && !cancelled) {
                    val nextKeyCopy = nextKey
                    if (nextKeyCopy == null || position == start + limit) {
                        s.onComplete()
                        return
                    }
                    val data = index.get(nextKeyCopy)
                    if (data != null) {
                        if (position >= start) {
                            val primaryKey: KeyWrapper
                            val value: V?
                            if (index == map.mvMap) {
                                value = map.codec.decode(data)
                                primaryKey = KeyWrapper(nextKeyCopy)
                            } else {
                                primaryKey = KeyWrapper(data)
                                value = map.read(primaryKey)
                            }
                            if (value != null) {
                                map.delete(value, primaryKey)
                                s.onNext(value)
                                count--
                                position++
                            }
                        } else
                            position++
                    }
                    if (nextKeyCopy.equals(lastKey)) {
                        s.onComplete()
                        return
                    }
                    // what if lastKey was removed from the index?
                    // check for exceeding bounds
                    if (reverse) {
                        nextKey = index.lowerKey(nextKeyCopy)
                        if (!lowerBound.isPrefixOf(nextKey) && lowerBound.compareTo(nextKey) > 0) {
                            s.onComplete()
                            return
                        }
                    } else {
                        nextKey = index.higherKey(nextKey)
                        if (!upperBound.isPrefixOf(nextKey) && upperBound.compareTo(nextKey) < 0) {
                            s.onComplete()
                            return
                        }

                    }
                }
            }
        })
    }
}
