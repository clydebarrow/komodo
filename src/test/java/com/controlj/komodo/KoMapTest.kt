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

import io.reactivex.subscribers.TestSubscriber
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test

class KoMapTest {

    class Coder : KoCodec<String> {
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
                        return KeyWrapper("1." + data)
                    }
                },
                object : KoCodec.Index<String> {
                    override val name: String = "second"

                    override val unique: Boolean = false

                    override fun keyGen(data: String): KeyWrapper {
                        return KeyWrapper("index2")
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
        range.forEach { assertTrue(map.read(inserted[it]!!) == "String $it") }
        // test for invalid index name
        try {
            map.query("noname")
            fail("Did not throw exception on invalid index name")
        } catch (e: Exception) {

        }
        run {
            val query = map.queryAsFlowable("first")
            val subscriber = TestSubscriber<String>()
            query.subscribe(subscriber)
            subscriber.await()
            subscriber.assertComplete()
            subscriber.assertValueCount(11)
        }

        run {
            val query = map.queryAsFlowable("first", start = 2, count = 2, reverse = true)
            val subscriber = TestSubscriber<String>()
            query.subscribe(subscriber)
            subscriber.await()
            subscriber.assertComplete()
            subscriber.assertValueCount(2)
            subscriber.assertValueAt(0, "String 7")
            subscriber.assertValueAt(1, "String 6")
        }

        run {
            val query = map.queryAsFlowable("first", lowerBound = KeyWrapper("1.String 10"), upperBound = KeyWrapper("1.String 8"), start = 2, count = 10)
            val subscriber = TestSubscriber<String>()
            query.subscribe(subscriber)
            subscriber.await()
            subscriber.assertComplete()
            subscriber.assertValueCount(6)
            (3..8).forEachIndexed { index, i ->
                subscriber.assertValueAt(index, "String $i")
            }
        }
    }
}
