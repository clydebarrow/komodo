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
    // the data is stored here
    private val mvMap: MVMap<Long, ByteArray>
    // the next primary key
    private val nextKey = AtomicLong()
    // lookup table for indices as provided by the codec
    private val indices = codec.indices.map { it.name to it }.toMap()
    // the maps for each index, lazily populated
    private val indexMaps: HashMap<String, MVMap<ByteArray, Long>> = HashMap()

    init {
        if (name.contains('.'))
            throw IllegalArgumentException("Map name may not contain '.'")
        val builder = MVMap.Builder<Long, ByteArray>()
        mvMap = store.store.openMap(name, builder)
        nextKey.set(mvMap.lastKey() ?: 0)
    }

    /**
     * Get the index map for the given name
     *
     */

    private fun getIndex(indexName: String): MVMap<ByteArray, Long> {
        return indexMaps.getOrPut(indexName, {store.store.openMap(fullIndexName(indexName))})

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
        val primaryKey: Long = nextKey.incrementAndGet()
        // if we have no indices, do a simple insert
        if (codec.indices.isEmpty()) {
            mvMap.put(primaryKey, codec.encode(data))
        } else {
            // otherwise begin a transaction, insert and update indices

            storeData(primaryKey, data)
        }
        return primaryKey
    }

    /**
     * Store data and update indices.
     * @param primaryKey The primary key to be used
     * @param data  The data to be stored
     * @param overwrite IF set, overwriting is permitted
     * @throws [DuplicateValueException] if a unique index is duplicated
     */
    private fun storeData(primaryKey: Long, data: Value, overwrite: Boolean = false) {

        //val transaction = store.transactionStore.begin()
        codec.indices.forEach { index ->
            val indexMap = getIndex(index.name)
            val old = indexMap.put(getKey(index, data, primaryKey), primaryKey)
            if (old != null && old != primaryKey && !overwrite) {
                //transaction.rollback()
                throw DuplicateValueException(index.name)
            }
        }
        //val valueMap = transaction.openMap<Long, ByteArray>(mvMap.name)
        mvMap.put(primaryKey, codec.encode(data))
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
     * Delete an object
     * @param primaryKey The primary key for the data
     */

    fun delete(primaryKey: Long) {
        val data = mvMap.get(primaryKey)
        if(data == null)
            return
        //val transaction = store.transactionStore.begin()
        codec.indices.forEach { index ->
            val indexMap = store.store.openMap<ByteArray, Long>(fullIndexName(index.name))
            indexMap.remove(getKey(index, codec.decode(data), primaryKey))
        }
        //val valueMap = transaction.openMap<Long, ByteArray>(mvMap.name)
        mvMap.remove(primaryKey)
        //transaction.commit()


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
        val key = ByteBuffer.allocate(keyValue.size + java.lang.Long.SIZE)
        key.put(keyValue)
        key.putLong(primaryKey)
        return key.array()
    }

    /**
     *  Create a query on this map using a specified index. The Query is a Flowable which can be subscribed to
     *  to access the returned objects. Objects will be delivered in keyvalue order for the specified index. If both
     *  key bounds and start/count values are provided, the start value is taken from the lower bound, i.e. the start and
     *  count values limit results *within* the key bounds.
     *
     *  @param indexName    The name of the index to use. Must be one of the indices provided by the associated CODEC
     *  @param lowerBound   (optional) A KeyWrapper the represents the lower key bound. The default is to start at the beginning of the index.
     *  @param upperBound   (optional) A KeyWrapper representing the upper key bound. The default is the end of the index
     *  @param start    (optional) The ordinal of the first result to deliver - the default is 0.
     *  @param count    (optional) The maximum number of results to deliver - the default is all of them
     *  @param reverse  IF true, the results will be delivered in reverse (descending) order. The default is false (ascending order)
     */

    fun query(
            indexName: String,
            lowerBound: KeyWrapper = KeyWrapper.START,
            upperBound: KeyWrapper = KeyWrapper.END,
            start: Int = 0,
            count: Int = Integer.MAX_VALUE,
            reverse: Boolean = false): Query<Value> {
        if(!indices.containsKey(indexName))
            throw UnknownIndexException(indexName)
        return Query(this, getIndex(indexName), lowerBound, upperBound, start, count, reverse)
    }
}