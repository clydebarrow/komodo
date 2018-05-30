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

import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.nio.ByteBuffer

class KoMapTest {

    class Coder : Codec<String> {
        override fun encode(data: String): ByteArray {
            return data.toByteArray()
        }

        override fun decode(encodedData: ByteArray): String {
            return String(encodedData)
        }

        override val indices: List<Codec.Index<String>> = listOf<Codec.Index<String>>(
                object : Codec.Index<String> {
                    override val name: String = "first"

                    override val unique: Boolean = false

                    override fun keyGen(data: String): KeyValue {
                        return KeyValue(data)
                    }
                })

    }

    class ListCoder : Codec<List<String>> {
        override fun encode(data: List<String>): ByteArray {
            val total = data.size * Integer.SIZE + data.sumBy { it.toByteArray().size }
            val buffer = ByteBuffer.allocate(total)
            data.forEach {
                val array = it.toByteArray()
                buffer.putInt(array.size)
                buffer.put(array)
            }
            return buffer.array()
        }

        override fun decode(encodedData: ByteArray): List<String> {
            val buffer = ByteBuffer.wrap(encodedData)
            val list = ArrayList<String>()
            while (buffer.hasRemaining()) {
                val length = buffer.getInt()
                val stringbuf = ByteArray(length)
                buffer.get(stringbuf)
                list.add(String(stringbuf))
            }
            return list
        }

        override val indices: List<Codec.Index<List<String>>> = listOf<Codec.Index<List<String>>>(
                object : Codec.Index<List<String>> {
                    override val name: String = "first"

                    override val unique: Boolean = false

                    override fun keyGen(data: List<String>): KeyValue {
                        val buffer = ByteBuffer.allocate(10)
                        data.take(1).forEach { buffer.put(it.toByteArray()) }
                        return KeyValue(buffer)
                    }
                })

    }

    lateinit var komodo: Komodo
    @Before
    @Throws(Exception::class)
    fun setUp() {
        komodo = Komodo()
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        komodo.close()
    }

    private val stringLists1: List<String> = listOf(
            "first data1",
            "second data",
            "third data°€↑"
    )
    private val stringLists: List<String> = listOf(
            "first data",
            "second data",
            "third data°€↑"
    )

    @Test
    fun komapTest() {
        try {
            komodo.koMap("name.more", Coder())
            fail("Illegal name not caught")
        } catch (ex: IllegalArgumentException) {

        }
        val map = komodo.koMap("test", Coder())
        val range = 0..10
        val inserted = range.map { it to map.insert("String $it") }.toMap()
        range.forEach { assertTrue(inserted[it] == it + 1L) }
        range.forEach { assertTrue(map.retrieve(inserted[it]!!) == "String $it") }
        // test for invalid index name
        try {
            map.query("noname")
            fail("Did not throw exception on invalid index name")
        } catch (e: Exception) {

        }
        var query = map.query("first")
        query.count().subscribe({
            assertEquals(range.count(), it.toInt())
        }, {
            fail(it.toString())
        })
        var cnt = 7
        query = map.query("first", start = 2, count = 2, reverse = true)
        query.subscribe({
            assertEquals("String $cnt", it)
            cnt--
        }, {
            fail(it.toString())
        }, {
            assertEquals(5, cnt)
        })

        cnt = 3
        query = map.query("first", lowerBound = KeyValue("String 10"), upperBound = KeyValue("String 8"), start = 2, count = 10)
        query.subscribe({
            println("cnt = $cnt, result = $it")
            assertEquals("String $cnt", it)
            cnt++
        }, {
            fail(it.toString())
        }, {
            assertEquals(9, cnt)
        })

        val listMap = komodo.koMap("lists", ListCoder())
        listMap.insert(stringLists)
    }
}