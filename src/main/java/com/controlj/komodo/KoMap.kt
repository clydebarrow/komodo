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
import java.util.concurrent.ConcurrentHashMap

/**
 *
 *  A map containing a specific kind of object. It requires a CODEC object to encode and decode
 *  objects into the underlying store
 *
 *  Create instances of this class by calling Komodo#koMap()
 *
 *  Multiple named indices may provided by the codec. The codec will calculate an index key for a given index name and object.
 *  At least one index must be provided, and the first index in the list will be used as the primary key, so it must be unique
 *
 */
class KoMap<Value> internal constructor(private val store: Komodo, val name: String, val codec: KoCodec<Value>) {
    internal val mvMap: MVMap<ByteArray, ByteArray>
    private val primaryIndex: KoCodec.Index<Value>
    // lookup table for indices as provided by the codec
    private val indices = codec.indices.map { it.name to it }.toMap()
    // the maps for each index, lazily populated
    private val indexMaps: ConcurrentHashMap<String, MVMap<ByteArray, ByteArray>> = ConcurrentHashMap()
    // a copy of the index list for our benefit
    private val indexList: List<KoCodec.Index<Value>>

    init {
        if (name.contains('.'))
            throw IllegalArgumentException("Map name may not contain '.'")
        if (codec.indices.isEmpty())
            throw IllegalArgumentException("At least one index is required")
        if (!codec.indices.first().unique)
            throw IllegalArgumentException("The primary index must be unique")
        primaryIndex = codec.indices.first()
        indexList = codec.indices.subList(1, codec.indices.size)
        mvMap = getIndex(primaryIndex.name)
    }

    /**
     * Get the index map for the given name
     *
     */

    private fun getIndex(indexName: String): MVMap<ByteArray, ByteArray> {
        return indexMaps.getOrPut(indexName, { store.store.openMap(fullIndexName(indexName)) })

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

    fun insert(data: Value): KeyWrapper {
        val primaryKey = primaryIndex.keyGen(data)
        if (mvMap.containsKey(primaryKey.byteArray))
            throw DuplicateValueException("Duplicate value for index ${primaryIndex.name}")
        val keyList = indexList.map { index ->
            val secKey = index.keyGen(data)
            if (index.unique) {
                val indexMap = getIndex(index.name)
                if (indexMap.containsKey(secKey.byteArray))
                    throw DuplicateValueException("Duplicate value for index ${index.name}")
            }
            secKey.byteArray
        }.toList()
        mvMap.put(primaryKey.byteArray, codec.encode(data))
        indexList.forEachIndexed { i, index ->
            val indexMap = getIndex(index.name)
            indexMap.put(keyList[i], primaryKey.byteArray)
        }
        return primaryKey
    }

    /**
     * Retrieve an object by primary key
     *
     */
    fun read(key: ByteArray): Value? {
        val data = mvMap.get(key)
        if (data != null) {
            val result = codec.decode(data)
            return result
        }
        return null
    }

    fun read(key: KeyWrapper): Value? {
        return read(key.byteArray)
    }

    /**
     * Retrieve an object. IF not in the map, create a default value and insert it. It is assumed
     * but not checked that the default value will have the given key
     */
    fun readOrCreate(key: KeyWrapper, default: () -> Value): Value {
        val result = read(key)
        if (result != null)
            return result
        val data = default()
        insert(data)
        return data
    }

    /**
     * Update an object. Store it if not already there
     * @param value  The object to be stored
     * @return The primary key of the object after update/insert
     */

    fun update(value: Value): KeyWrapper {
        val primaryKey = primaryIndex.keyGen(value)
        val oldData = mvMap.get(primaryKey.byteArray)
        if (oldData == null)
            return insert(value)
        val oldValue = codec.decode(oldData)
        mvMap.put(primaryKey.byteArray, codec.encode(value))
        indexList.forEach { index ->
            val indexMap = getIndex(index.name)
            val oldKey = getKey(index, oldValue, primaryKey)
            val newKey = getKey(index, value, primaryKey)
            if (newKey.compareTo(oldKey) != 0) {
                indexMap.remove(oldKey.byteArray)
                indexMap.put(newKey.byteArray, primaryKey.byteArray)
            }
        }
        return primaryKey
    }

    /**
     * Delete an object
     * @param primaryKey The primary key for the data
     */

    fun delete(primaryKey: KeyWrapper) {
        val data = mvMap.get(primaryKey.byteArray)
        if (data == null)
            return
        delete(codec.decode(data), primaryKey)
    }
    internal fun delete(data: Value, primaryKey: KeyWrapper) {
        //val transaction = store.transactionStore.begin()
        indexList.forEach { index ->
            val indexMap = store.store.openMap<ByteArray, Long>(fullIndexName(index.name))
            indexMap.remove(getKey(index, data, primaryKey).byteArray)
        }
        //val valueMap = transaction.openMap<Long, ByteArray>(mvMap.name)
        mvMap.remove(primaryKey.byteArray)
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
    private fun getKey(index: KoCodec.Index<Value>, data: Value, primaryKey: KeyWrapper): KeyWrapper {
        val partial = index.keyGen(data)
        if (index.unique)
            return partial
        return addPrimaryKey(partial, primaryKey)
    }

    private fun addPrimaryKey(keyValue: KeyWrapper, primaryKey: KeyWrapper): KeyWrapper {
        val key = ByteArray(keyValue.byteArray.size + primaryKey.byteArray.size)
        System.arraycopy(keyValue, 0, key, 0, keyValue.byteArray.size)
        System.arraycopy(primaryKey, keyValue.byteArray.size, key, 0, primaryKey.byteArray.size)
        return KeyWrapper(key)
    }

    /**
     *  Create a query on this map using a specified index. The Query is a Flowable which can be subscribed to
     *  to access the returned objects. Objects will be delivered in keyvalue order for the specified index. If both
     *  key bounds and start/count values are provided, the start value is taken from the lower bound, i.e. the start and
     *  count values limit results *within* the key bounds.
     *
     *  @param indexName    The name of the index to use. Must be one of the indices provided by the associated CODEC
     *                      The default is the primary key index
     *  @param lowerBound   (optional) A KeyWrapper the represents the lower key bound. The default is to start at the beginning of the index.
     *  @param upperBound   (optional) A KeyWrapper representing the upper key bound. The default is the end of the index
     *  @param start    (optional) The ordinal of the first result to deliver - the default is 0.
     *  @param count    (optional) The maximum number of results to deliver - the default is all of them
     *  @param reverse  IF true, the results will be delivered in reverse (descending) order. The default is false (ascending order)
     */

    fun query(
            indexName: String = primaryIndex.name,
            lowerBound: KeyWrapper = KeyWrapper.START,
            upperBound: KeyWrapper = KeyWrapper.END,
            start: Int = 0,
            count: Int = Integer.MAX_VALUE,
            reverse: Boolean = false): Query<Value> {
        if (!indices.containsKey(indexName))
            throw UnknownIndexException(indexName)
        return Query(this, getIndex(indexName), lowerBound, upperBound, start, count, reverse)
    }

    /**
     *  Create a delete operation on this map using a specified index. The Query is a Flowable which can be subscribed to
     *  to access the now deleted objects. Objects will be delivered in keyvalue order for the specified index. If both
     *  key bounds and start/count values are provided, the start value is taken from the lower bound, i.e. the start and
     *  count values limit results *within* the key bounds.
     *
     *  @param indexName    The name of the index to use. Must be one of the indices provided by the associated CODEC
     *                      The default is the primary key index
     *  @param lowerBound   A KeyWrapper the represents the lower key bound. No default
     *  @param upperBound   A KeyWrapper representing the upper key bound. No default
     *  @param start    (optional) The ordinal of the first result to deliver - the default is 0.
     *  @param count    (optional) The maximum number of results to deliver - the default is all of them
     *  @param reverse  IF true, the results will be delivered in reverse (descending) order. The default is false (ascending order)
     */

    fun delete(
            indexName: String = primaryIndex.name,
            lowerBound: KeyWrapper,
            upperBound: KeyWrapper,
            start: Int = 0,
            count: Int = Integer.MAX_VALUE,
            reverse: Boolean = false): Delete<Value> {
        if (!indices.containsKey(indexName))
            throw UnknownIndexException(indexName)
        return Delete(this, getIndex(indexName), lowerBound, upperBound, start, count, reverse)
    }

}
