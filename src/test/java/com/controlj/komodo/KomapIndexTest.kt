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
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.nio.ByteBuffer

class KomapIndexTest {

    class Coder: KoCodec<String> {
        override fun encode(data: String, primaryKey: KeyWrapper): ByteArray {
            return data.toByteArray()
        }

        override fun decode(encodedData: ByteArray, primaryKey: KeyWrapper?): String {
            return String(encodedData)
        }

        override val indices: List<KoCodec.Index<String>>
            get() = listOf()

    }

    class Coder1: KoCodec<String> {
        override fun encode(data: String, primaryKey: KeyWrapper): ByteArray {
            return data.toByteArray()
        }

        override fun decode(encodedData: ByteArray, primaryKey: KeyWrapper?): String {
            return String(encodedData)
        }

        override val indices: List<KoCodec.Index<String>> = listOf(
        object : KoCodec.Index<String> {
            override val name: String = "first"

            override val unique: Boolean = false

            override fun keyGen(data: String): KeyWrapper {
                return KeyWrapper(data)
            }
        })
    }
    class Coder2: KoCodec<String> {
        override fun encode(data: String, primaryKey: KeyWrapper): ByteArray {
            return data.toByteArray()
        }

        override fun decode(encodedData: ByteArray, primaryKey: KeyWrapper?): String {
            return String(encodedData)
        }

        override val indices: List<KoCodec.Index<String>> = listOf(
                object : KoCodec.Index<String> {
                    override val name: String = "first"

                    override val unique: Boolean = true

                    override fun keyGen(data: String): KeyWrapper {
                        return KeyWrapper(data)
                    }
                })
    }
    class ListCoder: KoCodec<List<String>> {
        override fun encode(data: List<String>, primaryKey: KeyWrapper): ByteArray {
            val total = data.size * Integer.SIZE + data.sumBy { it.toByteArray().size }
            val buffer = ByteBuffer.allocate(total)
            data.forEach{
                val array = it.toByteArray()
                buffer.putInt(array.size)
                buffer.put(array)
            }
            return buffer.array()
        }

        override fun decode(encodedData: ByteArray, primaryKey: KeyWrapper?): List<String> {
            val buffer = ByteBuffer.wrap(encodedData)
            val list = ArrayList<String>()
            while(buffer.hasRemaining()) {
                val length = buffer.int
                val stringbuf = ByteArray(length)
                buffer.get(stringbuf)
                list.add(String(stringbuf))
            }
            return list
        }

        override val indices: List<KoCodec.Index<List<String>>>
            get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

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

    @Test
    fun komapTest() {
        try {
            komodo.koMap("name.more", Coder())
            fail("Illegal name not caught")
        } catch (ex: IllegalArgumentException) {

        }
        try {
            komodo.koMap("test", Coder())
            fail("Illegal index list not caught")
        } catch (ex: IllegalArgumentException) {}

        try {
            komodo.koMap("test", Coder1())
            fail("Illegal index list not caught")
        } catch (ex: IllegalArgumentException) {}

        val map = komodo.koMap("test", Coder2())
        val range = 0 .. 10
        val inserted = range.map{ it to map.insert("String $it") }.toMap()
        range.forEach { assertTrue(map.read(inserted[it]!!) == "String $it")  }
    }
}
