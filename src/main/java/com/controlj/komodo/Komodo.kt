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

import com.controlj.komodo.exceptions.KomodoException
import org.h2.mvstore.MVStore
import org.h2.mvstore.tx.TransactionStore

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 *
 * User: clyde
 * Date: 29/5/18
 * Time: 11:38
 */
class Komodo(filename: String = "", compressed: Boolean = false, encryptionKey: String = "") {

    internal val store: MVStore

    init {
        val builder = MVStore.Builder()
        if (filename.isNotBlank())
            builder.fileName(filename)
        if (compressed)
            builder.compress()
        if (encryptionKey.isNotBlank())
            builder.encryptionKey(encryptionKey.toCharArray())
        store = builder.open()
    }

    internal val transactionStore: TransactionStore by lazy { TransactionStore(store) }

    fun <Value> koMap(name: String, codec: KoCodec<Value>): KoMap<Value> {
        return KoMap(this, name, codec)
    }

    fun deleteMap(name: String) {
        store.removeMap(name)
    }

    fun commit() {
        store.commit()
    }

    fun close() {
        if (transactionStore.openTransactions.isNotEmpty())
            throw KomodoException("Closing store with open transactions")
        store.close()
    }


}

val Int.Companion.BYTES: Int get() = 4
val Float.Companion.BYTES: Int get() = 4
val Double.Companion.BYTES: Int get() = 8
val Long.Companion.BYTES: Int get() = 8
