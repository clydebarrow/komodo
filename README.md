# Komodo
A Kotlin noSQL database with MVStore as the backend
## Why?
This project was started fill a need for a lightweight, efficient database in Kotlin that could be deployed anywhere a JVM is available.

The key objectives are:
* As few dependencies as possible
* No SQL
* Local file backend
* Reactive API
* Kotlin idioms
* B-tree and R-tree indices

## Backend
The underlying store is MVStore, from the H2 database project. This is a production quality, self-contained pure Java implementation of key-value stores. A version of MVStore compiled for Java 7 (and thus Android compatible) is included in the Komodo jar file so no dependencies are required.
## POKO (Plain Old Kotlin Objects) interface
Each KoMap (equivalent to a table) in the database is intended to store one kind of object. The translation between the stored data (which will usually be in the form
of simple arrays of bytes) and Kotlin objects is done by a a CODEC (Coder/decoder) which is specific to each object. There is no default 
serialization or mapping - a CODEC must be written for each KoMap. The CODEC is also responsible for specifying any indices used. Consequently
Komodo does not depend on GSON, Jackson or any other serialization framework, however any standard serialisation technique can be used.
## Indices
The KoMap itself is a map of primary keys (Long data type) to objects. Indices are implemented by creating secondary maps of keys (as generated 
by the CODEC) to primary keys. There can be multiple indices for each KoMap and all insertions, deletions and updates will update all indices
automatically. Unique and non-unique indices are supported.

## Usage

Komodo is published on Maven Central. Include in your gradle project:

````
dependencies {
    implementation "com.control-j:komodo:1.0.1"
}
````

A sample class follows:
package com.controlj.data

import com.controlj.komodo.KeyWrapper
import com.controlj.komodo.KoCodec
import com.controlj.komodo.KoMap
import com.controlj.komodo.Komodo
import com.google.gson.Gson

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 * All rights reserved
 *
 * User: clyde
 * Date: 2019-05-31
 * Time: 15:01
 *
 * A class used to save info about a connected device
 *
 * @param key The device serial number
 * @param runNumber The last run number seen
 * @param model The device model string
 * @param name The device name
 * @param version The firmware version number
 * @param description A descriptive string
 */
data class Sample(
        val key: Long,
        val address: String,
        val serial: Int,
        var description: String,
        ) {
    fun save() {
        diMap.update(this)
    }

    fun delete() {
        diMap.delete(DeviceCodec.keyFor(this))
    }

    override fun toString(): String {
        return "Sample($name: S/N=$serial, $description)"
    }

    companion object {
        // the database would usually be provided externally
        val database by lazy { Komodo(File(".", "Sample").path) }
        private const val DB_MAP_NAME = "Sample"        // the name of the Komap
        private const val DB_INDEX_PRIMARY = "primary"  // primary key name
        private const val DB_INDEX_ADDRESS = "address"  // secondary key name
        internal val diMap: KoMap<Sample> by lazy { database.koMap(DB_MAP_NAME, DeviceCodec) }

        /**
         * Retrieve all the Sample objects in the database
         */
        fun getEntries(): Iterable<Sample> {
            return diMap.query(DB_INDEX_PRIMARY)
        }

        /**
         * Retrieve a single Sample by key
         */

        fun get(deviceKey: Long): Sample? {
            return diMap.read(DeviceCodec.keyFor(deviceKey))
        }

        /**
         *  retrieve a single sample by address
         */
        fun get(address: String): Sample? {
            val key = DeviceCodec.keyFor(address)
            return diMap.query(DB_INDEX_ADDRESS, key, key).firstOrNull()
        }

        /**
         * The Codec. It defines a primary and a secondary key. Gson is used as the serialiser
         */
        object DeviceCodec : KoCodec<Sample> {
            private val mapper = Gson()
            override fun encode(data: Sample, primaryKey: KeyWrapper): ByteArray {
                return mapper.toJson(data).toByteArray()
            }

            override fun decode(encodedData: ByteArray, primaryKey: KeyWrapper?): Sample {
                return mapper.fromJson(String(encodedData), Sample::class.java)
            }

            /**
             * Generate a KeyWrapper for the primary key
             */
            fun keyFor(sampleKey: Long): KeyWrapper {
                return KeyWrapper.of(sampleKey)
            }

            /**
             * Generate a key for the secondary key
             */
            fun keyFor(address: String): KeyWrapper {
                return KeyWrapper.of(address)
            }

            /**
             * Create a Keywrapper for a an object
             */
            fun keyFor(devInfo: Sample): KeyWrapper {
                return keyFor(devInfo.key)
            }

            /**
             * The list of indices
             */
            override val indices: List<KoCodec.Index<Sample>> = listOf(
                    // Primary index
                    object : KoCodec.Index<Sample> {
                        override val name: String = DB_INDEX_PRIMARY
                        override val unique: Boolean = true
                        override fun keyGen(data: Sample): KeyWrapper {
                            return keyFor(data)
                        }
                    },
                    // secondary index
                    object : KoCodec.Index<Sample> {
                        override val name: String = DB_INDEX_ADDRESS
                        override val unique: Boolean = true
                        override fun keyGen(data: Sample): KeyWrapper {
                            return keyFor(data.address)
                        }
                    }
            )
        }
    }
}
````

To be continued....
