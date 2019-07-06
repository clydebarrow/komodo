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

import org.threeten.bp.Instant
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 *
 * User: clyde
 * Date: 30/5/18
 * Time: 13:04
 *
 * This class wraps a key. Convenience methods are provided to wrap simple strings and integers
 *
 */
open class KeyWrapper {
    val byteArray: ByteArray

    constructor(byteArray: ByteArray) {
        this.byteArray = byteArray
    }

    constructor(buffer: ByteBuffer) : this(buffer.array())

    constructor(string: String) : this(string.toByteArray())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        return other is KeyWrapper && byteArray.isNotEmpty() && byteArray.contentEquals(other.byteArray)
    }

    override fun hashCode(): Int {
        return byteArray.sum()
    }

    /**
     * Is this value a prefix of the compared key?
     *
     */

    fun isPrefixOf(other: ByteArray?): Boolean {
        if (other == null)
            return false
        if (other.size < byteArray.size)
            return false
        byteArray.forEachIndexed { index, byte -> if (other[index] != byte) return false }
        return true
    }

    fun isPrefixOf(other: KeyWrapper): Boolean {
        return isPrefixOf(other.byteArray)
    }

    /**
     * Compare one key to another. Return <0, 0 or >0 depending on
     * whether this object is <, = or > the other respectively
     * @param other The other key to compare to
     * @return -1, 0 or 1
     */
    open fun compareTo(other: ByteArray?): Int {
        if (other == null)
            return 1
        return byteArray.compareTo(other)
    }

    /**
     * Compare one key to another. Return <0, 0 or >0 depending on
     * whether this object is <, = or > the other respectively
     * @param other A KeyWrapper containing the other key to compare to
     * @return -1, 0 or 1
     */

    fun compareTo(other: KeyWrapper): Int {
        if (other == START)
            return 1
        if (other == END)
            return -1
        return compareTo(other.byteArray)
    }

    companion object {

        /**
         * Create a key from a number of primitive objects
         * @param keys  The keys to be used. Will be stored in big-endian format, in the given order, so
         *  the first key is the most significant.
         */

        fun of(vararg keys: Any): KeyWrapper {
            if (keys.isEmpty())
                return START
            val size = keys.map {
                when (it) {
                    is Int -> Int.SIZE_BYTES
                    is Long, is Instant -> Long.SIZE_BYTES
                    is String -> it.toByteArray().size
                    else -> throw IllegalArgumentException("Illegal key class ${it::class}")
                }
            }.sum()
            val buffer = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN)
            keys.forEach {
                when (it) {
                    is Int -> buffer.putInt(it)
                    is Long -> buffer.putLong(it)
                    is Instant -> buffer.putLong(it.toEpochMilli())
                    is String -> buffer.put(it.toByteArray())
                    else -> Unit
                }
            }
            return KeyWrapper(buffer)
        }
        /**
         * Sentinel values used to represent the start and end of a KoMap
         */
        /**
         * This value represents the start of a an index. It always compares as less
         * than any other key except itself
         */
        val START = object : KeyWrapper(byteArrayOf()) {
            override fun compareTo(other: ByteArray?): Int {
                if (other === byteArray)
                    return 0
                return -1
            }
        }
        /**
         * This value represents the end of a an index. It always compares as greater
         * than any other key except itself
         */
        val END = object : KeyWrapper(byteArrayOf()) {
            // END is equal to itself, but bigger than anything else
            override fun compareTo(other: ByteArray?): Int {
                if (other === byteArray)
                    return 0
                return 1
            }
        }
    }
}

fun ByteArray.compareTo(other: ByteArray?): Int {
    if(other == null)
        return 1
    forEachIndexed { index, byte ->
        // if we are longer, we are bigger at this point
        if (index == other.size)
            return 1
        val ob = other[index]
        if (ob != byte) return byte - ob
    }
    // we are a prefix or equal. So...
    if (other.size > size)
        return -1
    return 0
}

fun ByteArray?.equals(other: ByteArray?): Boolean {
    if (this === other)
        return true
    if (this == null || other == null)
        return false
    return contentEquals(other)
}
