/*
 * Copyright 2018 Control-J Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.controlj.komodo

import com.controlj.komodo.exceptions.DuplicateValueException
import com.controlj.komodo.exceptions.UnknownIndexException
import org.h2.mvstore.MVMap
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

/**
 *
 *  A map containing a specific kind of object. It requires a CODEC object to encode and decode
 *  objects into the underlying store
 *
 *  Create instances of this class by calling Komodo#koMap()
 *
 *  Multiple named indices may provided by the codec. The codec will calculate an index key for a given index name and object.
 *
 */
class KoMap<Value> internal constructor(private val store: Komodo, val name: String, val codec: Codec<Value>) {
    private val mvMap: MVMap<Long, ByteArray>
    private val nextKey = AtomicLong()
    private val indexMap = codec.indices.map { it.name to it }.toMap()

    init {
        if (name.contains('.'))
            throw IllegalArgumentException("Map name may not contain '.'")
        val builder = MVMap.Builder<Long, ByteArray>()
        mvMap = store.store.openMap(name, builder)
        nextKey.set(mvMap.lastKey() ?: 0)
    }

    /**
     * Insert an object into the map
     *
     *
     * @param data The object to be inserted
     * @return the primary key of the resulting entry
     * @throws [DuplicateValueException] if there is a clash in a unique index
     *
     */

    fun insert(data: Value): Long {
        val keyValue: Long
        keyValue = nextKey.incrementAndGet()
        // if we have no indices, do a simple insert
        if (codec.indices.isEmpty()) {
            mvMap.put(keyValue, codec.encode(data))
        } else {
            // otherwise begin a transaction, insert and update indices

            updateIndices(keyValue, data)
        }
        return keyValue
    }

    /**
     * Retrieve an index map for a given name
     *
     */

    internal fun getIndex(name: String): MVMap<ByteArray, Long> {
        return store.store.openMap(fullIndexName(name))
    }
    /**
     * Store data and update indices.
     * @param keyValue The primary key to be used
     * @param data  The data to be stored
     * @param overwrite IF set, overwriting is permitted
     * @throws [DuplicateValueException] if a unique index is duplicated
     */
    private fun updateIndices(keyValue: Long, data: Value, overwrite: Boolean = false) {

        //val transaction = store.transactionStore.begin()
        codec.indices.forEach { index ->
            val indexMap = store.store.openMap<ByteArray, Long>(fullIndexName(index.name))
            val old = indexMap.put(getKey(index, data, keyValue), keyValue)
            if (old != null && old != keyValue && !overwrite) {
                //transaction.rollback()
                throw DuplicateValueException(index.name)
            }
        }
        //val valueMap = transaction.openMap<Long, ByteArray>(mvMap.name)
        mvMap.put(keyValue, codec.encode(data))
        //transaction.commit()
    }

    /**
     * Retrieve an object by primary key
     *
     */
    fun retrieve(key: Long): Value? {
        val data = mvMap.get(key)
        if (data != null)
            return codec.decode(data)
        return null
    }

    /**
     * Get the full name of the index map. This is the name of the primary map joined to the
     * index name with a dot. Hence dots are not allowed in primary map names
     */
    private fun fullIndexName(indexName: String): String {
        return "$name.$indexName"
    }

    /**
     * Get the key for a given index and data item. Append the primary key if the index
     * allows multiple entries
     */
    private fun getKey(index: Codec.Index<Value>, data: Value, primaryKey: Long = 0): ByteArray {
        val partial = index.keyGen(data)
        if(index.unique)
            return partial.byteArray
        return addPrimaryKey(partial.byteArray, primaryKey)
    }

    private fun addPrimaryKey(keyValue: ByteArray, primaryKey: Long): ByteArray {
        val key = ByteBuffer.allocate(keyValue.size + 8)
        key.put(keyValue)
        key.putLong(primaryKey)
        return key.array()
    }

    /**
     *
     */

    fun query(
            indexName: String,
            lowerBound: KeyValue = KeyValue.START,
            upperBound: KeyValue = KeyValue.END,
            start: Int = 0,
            count: Int = Integer.MAX_VALUE,
            reverse: Boolean = false): Query<Value> {
        if(!indexMap.containsKey(indexName))
            throw UnknownIndexException(indexName)
        return Query(this, getIndex(indexName), lowerBound, upperBound, start, count, reverse)
    }
}