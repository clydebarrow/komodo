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
 * A query on a map using a given index with specified bounds.
 * @param map   The KoMap to be queried
 * @param index The index to be used
 * @param lowerBound    The lower bound KeyValue. The default is the first key in the index
 * @param upperBound    The upper bound KeyValue. The default is the last key in the index
 * @param start    A start position - this many values will be skipped before any are emitted
 * @param limit The maximum number of values to be emitted
 * @param reverse If set, the values will be emitted in reverse order, i.e. starting with the upperBound
 */
class  Query<V> internal constructor(
        private val map: KoMap<V>,
        private val index: MVMap<ByteArray, Long>,
        private val lowerBound: KeyValue,
        private val upperBound: KeyValue,
        private val start: Int,
        private val limit: Int,
        private val reverse: Boolean
        ): Flowable<V>() {

    private var position = 0
    private var nextKey: ByteArray? = null
    private var lastKey: ByteArray? = null

    override fun subscribeActual(s: Subscriber<in V>) {
        val upperKey = when(upperBound) {
            KeyValue.END -> index.lastKey()
            KeyValue.START -> index.firstKey()
            else -> {
                val last = index.ceilingKey(upperBound.byteArray)
                if(upperBound.isPrefixOf(last)) last else index.lowerKey(last)
            }
        }
        val lowerKey = when(lowerBound) {
            KeyValue.END -> index.lastKey()
            KeyValue.START -> index.firstKey()
            else -> index.ceilingKey(lowerBound.byteArray)
        }
        if(reverse) {
            nextKey = upperKey
            lastKey = lowerKey
        } else {
            nextKey = lowerKey
            lastKey = upperKey
        }

        s.onSubscribe(object: Subscription {
            override fun cancel() {
            }

            override fun request(n: Long) {
                var count = n
                while(count != 0L) {
                    if(nextKey == null || position == start+limit) {
                        s.onComplete()
                        return
                    }
                    val primaryKey = index.get(nextKey)
                    primaryKey?.apply {
                        if(position >= start) {
                            val value = map.retrieve(this)
                            if(value != null) {
                                s.onNext(value)
                                count--
                                position++
                            }
                        } else
                            position++
                    }
                    if(nextKey!!.equals(lastKey)) {
                        s.onComplete()
                        return
                    }
                    nextKey = if(reverse) index.lowerKey(nextKey) else index.higherKey(nextKey)
                }
            }
        })
    }
}