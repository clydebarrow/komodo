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

import io.reactivex.Single
import io.reactivex.SingleObserver
import io.reactivex.disposables.Disposable
import org.h2.mvstore.MVMap

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 13:19
 *
 * Count a set of items from a map via a query.
 * @param map   The KoMap to be used
 * @param index The index to be used
 * @param lowerBound    The lower bound KeyValue. No default
 * @param upperBound    The upper bound KeyValue. No default
 */
class Count internal constructor(
        private val map: KoMap<*>,
        private val index: MVMap<ByteArray, ByteArray>,
        private val lowerBound: KeyWrapper = KeyWrapper.START,
        private val upperBound: KeyWrapper = KeyWrapper.END
        ) : Single<Long>() {


    override fun subscribeActual(s: SingleObserver<in Long>) {
        var disposed = false
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

        s.onSubscribe(object : Disposable {

            override fun isDisposed(): Boolean {
                return disposed
            }

            override fun dispose() {
                disposed = true
            }
        })
        var nextKey = lowerKey

        var count: Long = 0
        while (!disposed) {
            if (nextKey == null)
                break
            count++
            if (nextKey.equals(upperKey)) {
                break
            }
            // what if lastKey was removed from the index?
            // check for exceeding bounds
            nextKey = index.higherKey(nextKey)
            if (!upperBound.isPrefixOf(nextKey) && upperBound.compareTo(nextKey) < 0)
                break
        }
        s.onSuccess(count)
    }
}
