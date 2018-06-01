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

/**
 * Encode and decode an object to a byte array
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 04:34
 */
interface KoCodec<T> {
    /**
     * Encode the data to a byte array
     */
    fun encode(data: T): ByteArray

    /**
     * Given a byte array, decode it to an object of type T
     */
    fun decode(encodedData: ByteArray): T

    /**
     * Return a list of the indices for this codec
     *
     */

    val indices: List<Index<T>>

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
}