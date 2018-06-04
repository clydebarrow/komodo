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

import org.h2.mvstore.MVMap

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 13:19
 *
 * A query on a map using a given index with specified bounds.
 * @param map   The KoMap to be queried
 * @param index The index to be used
 * @param lowerBound    The lower bound KeyValue. The default is the first key in the index
 * @param upperBound    The upper bound KeyValue. The default is the last key in the index
 * @param start    A start position - this many values will be skipped before any are emitted
 * @param limit The maximum number of values to be emitted
 * @param reverse If set, the values will be emitted in reverse order, i.e. starting with the upperBound
 */
class Query<V> internal constructor(
        private val map: KoMap<V>,
        private val index: MVMap<ByteArray, ByteArray>,
        private val lowerBound: KeyWrapper,
        private val upperBound: KeyWrapper,
        private val start: Int,
        private val limit: Int,
        private val reverse: Boolean
) : Iterator<V> {

    val firstKey: ByteArray?
    val lastKey: ByteArray?

    init {
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
    }

    var nextKey = firstKey
    var position = 0
    override fun hasNext(): Boolean {
        return nextKey != null && position != start + limit
    }

    override fun next(): V {
        if (!hasNext())
            throw NoSuchElementException()
        var value: V? = null
        do {


            val data = index.get(nextKey)
            if (data != null) {
                if (position < start) {
                    position++
                } else {
                    value =
                            if (index != map.mvMap)
                                map.read(data)
                            else
                                map.codec.decode(data)
                }
                if (nextKey!!.equals(lastKey)) {
                    nextKey = null
                } else {
                    // what if lastKey was removed from the index?
                    // check for exceeding bounds
                    if (reverse) {
                        nextKey = index.lowerKey(nextKey)
                        if (!lowerBound.isPrefixOf(nextKey) && lowerBound.compareTo(nextKey) > 0) {
                            nextKey = null
                        }
                    } else {
                        nextKey = index.higherKey(nextKey)
                        if (!upperBound.isPrefixOf(nextKey) && upperBound.compareTo(nextKey) < 0) {
                            nextKey = null
                        }

                    }
                }
            }
        } while(value == null)
        position++
        return value
    }
}
