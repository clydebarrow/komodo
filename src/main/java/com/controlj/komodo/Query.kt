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
 * @param stride the key interval; 1 means return every entry, 2 means return every second entry etc.
 */
class Query<V : Any> internal constructor(
        private val map: KoMap<V>,
        private val index: MVMap<ByteArray, ByteArray>,
        private val lowerBound: KeyWrapper,
        private val upperBound: KeyWrapper,
        private val start: Int,
        private val limit: Int,
        private val reverse: Boolean,
        private val stride: Int = 1
) : Iterator<V> {

    val firstKey: ByteArray?
    val lastKey: ByteArray?
    private var nextKey: ByteArray? = null
    private var position = 0

    init {
        if (stride <= 0)
            throw IllegalArgumentException("Stride must be > 0")
        val upperKey = when (upperBound) {
            KeyWrapper.END -> index.lastKey()
            KeyWrapper.START -> index.firstKey()
            else -> index.floorKey(upperBound.byteArray)
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
        if (lastKey != null) {
            nextKey = firstKey
            while(position < start && nextKey != null)
                skipToNextKey()
        }
    }

    override fun hasNext(): Boolean {
        return nextKey != null && position < start + limit
    }

    // move the key position to the next key in the desired sequence.
    private fun skipToNextKey() {
        // are we already at the end?
        if (nextKey == null)
            return
        // have we reached the end, either by query limits or count?
        if (position >= start + limit || nextKey.equals(lastKey)) {
            nextKey = null
            return
        }
        // get next key and compare to upper key bound
        if (reverse) {
            nextKey = index.lowerKey(nextKey)
            if (lowerBound.compareTo(nextKey) > 0) {
                nextKey = null
            }
        } else {
            nextKey = index.higherKey(nextKey)
            if (upperBound.compareTo(nextKey) < 0) {
                nextKey = null
            }
        }
        if (nextKey != null)
            position++
    }

    override fun next(): V {
        if (!hasNext())
            throw NoSuchElementException()
        val value: V = (index.get(nextKey))?.let {
            if (index != map.mvMap)
                map.read(KeyWrapper(it))
            else
                map.codec.decode(it, KeyWrapper(nextKey!!))

        } ?: throw NoSuchElementException(
                "Unexpected failure to find next element, position=$position, limit=${start + limit}")
        repeat(stride) { skipToNextKey() }
        return value
    }
}


