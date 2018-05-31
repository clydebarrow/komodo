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

import io.reactivex.FlowableSubscriber
import io.reactivex.schedulers.Schedulers
import io.reactivex.subscribers.TestSubscriber
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.reactivestreams.Subscription
import java.util.concurrent.TimeUnit

class DeleteTest {

    class Coder : Codec<String> {
        override fun encode(data: String): ByteArray {
            return data.toByteArray()
        }

        override fun decode(encodedData: ByteArray): String {
            return String(encodedData)
        }

        override val indices: List<Codec.Index<String>> = listOf<Codec.Index<String>>(
                object : Codec.Index<String> {
                    override val name: String = "index"

                    override val unique: Boolean = true

                    override fun keyGen(data: String): KeyWrapper {
                        return KeyWrapper(data)
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

    /**
     * test that an entry can be deleted, and that doing so in the middle of a query will result in valid results
     */
    @Test
    fun deleteTest() {
        val map = komodo.koMap("test", Coder())
        val range = 0..9
        val inserted = range.map { it to map.insert("String $it") }.toMap()
        assertEquals(10, inserted.size)
        val query = map.query("index", upperBound = KeyWrapper("String 7")).subscribeOn(Schedulers.io())
        val subscriber = TestSubscriber<String>(4)
        query.subscribe(subscriber)
        subscriber.awaitCount(3)
        map.delete(inserted[7]!!)
        subscriber.requestMore(100)
        subscriber.await(4, TimeUnit.SECONDS)
        subscriber.assertComplete()
        subscriber.assertValueCount(7)
        subscriber.assertValueAt(6, "String 6")
    }
    @Test
    fun delete2() {
        val map = komodo.koMap("test", Coder())
        val range = 0..9
        val inserted = range.map { it to map.insert("String $it") }.toMap()
        assertEquals(10, inserted.size)
        map.delete(inserted[3]!!)
        val query = map.query("index", upperBound = KeyWrapper("String 7")).subscribeOn(Schedulers.io())
        val subscriber = TestSubscriber<String>(4)
        query.subscribe(subscriber)
        subscriber.awaitCount(3)
        map.delete(inserted[2]!!)
        map.delete(inserted[6]!!)
        subscriber.requestMore(100)
        subscriber.await(4, TimeUnit.SECONDS)
        subscriber.assertComplete()
        subscriber.assertValueCount(6)
        subscriber.assertValueAt(5, "String 7")
        subscriber.assertValueAt(2, "String 2")
        //subscriber.values().forEach { println(it) }
    }
}