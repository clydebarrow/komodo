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

import org.h2.mvstore.rtree.SpatialKey
import org.threeten.bp.Instant

/**
 * Encode and decode an object to a byte array
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 04:34
 */
interface KoCodec<T : Any> {
    /**
     * Encode the data to a byte array. May be stored in a cache
     */
    fun encode(data: T, primaryKey: KeyWrapper): ByteArray

    /**
     * Given a byte array, decode it to an object of type T
     * @param encodedData The encoded version of the object
     * @param primaryKey The primary key for the object. If this is non-null, the object may be returned
     *  from a cache, or the decoded object stored in a cache. If null, the object should be removed from the cache.
     */
    fun decode(encodedData: ByteArray, primaryKey: KeyWrapper? = null): T

    /**
     * Return a list of the indices for this codec
     *
     */

    val indices: List<Index<T>>

    /**
     * A list of the spatial indices for this map.
     * May be empty
     */
    val spatialIndices: List<SpatialIndex<T>>
        get() = listOf()

    /**
     * An index type
     */
    interface Index<T> {
        /**
         * The name of the index
         */

        val name: String

        /**
         * Is this index unique?
         */

        val unique: Boolean

        /**
         * Generate a key for the index from the supplied data
         *
         * @param data The object from which to generate the key
         * @return The generated key as a byte array
         */
        fun keyGen(data: T): KeyWrapper

    }

    interface SpatialIndex<T> {
        /**
         * The name of the index
         */

        val name: String

        /**
         * Generate a spatial key from the object
         */

        fun keyGen(data: T): SpatialKey
    }

    companion object {

        val minInstant = Instant.EPOCH          // the minimum timestamp available
        val maxInstant  = Instant.ofEpochMilli(Long.MAX_VALUE)  // the maximum timestamp available
    }
}