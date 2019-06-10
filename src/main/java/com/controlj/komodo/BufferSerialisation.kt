package com.controlj.komodo

import org.threeten.bp.Instant
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 * All rights reserved
 *
 * User: clyde
 * Date: 2019-03-21
 * Time: 09:44
 *
 * Provide a means to serialise and deserialise an object to a bytebuffer.
 * The conversion is done in order of the parameters to the primary constructor of the object,
 * and all parameters must be primitive types.
 *
 *  All primitive types
 *  String  // stored as a 16 bit length followed by that many Unicode characters
 *  Other objects composed entirely of supported types
 *
 * the bytebuffers are packed - there is no padding to align on specific boundaries
 */
@Suppress("UNCHECKED_CAST")
object BufferSerialisation {

    /**
     * A list of the supported types and functions to handle them
     */
    private sealed class Type(val clazz: KClass<out Any>, val byteSize: Int = 0) {
        /**
         * Get the size required to store this object
         */
        open fun getBytes(obj: Any, field: KProperty1<Any, Any?>) = byteSize

        /**
         * Put the [obj] into the [buffer]
         */
        abstract fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>)

        /**
         * Get a field from the buffer
         */
        abstract fun get(buffer: ByteBuffer): Any

        object BOOLEAN : Type(Boolean::class, Byte.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Boolean {
                return buffer.get().toInt() != 0
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Boolean
                buffer.put((if (data) 1 else 0).toByte())
            }
        }

        object BYTE : Type(Byte::class, Byte.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Byte {
                return buffer.get()
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Byte
                buffer.put(data)
            }
        }

        object CHAR : Type(Char::class, Char.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Char {
                return buffer.char
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Char
                buffer.putChar(data)
            }
        }

        object SHORT : Type(Short::class, Short.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Short {
                return buffer.short
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Short
                buffer.putShort(data)
            }
        }

        object INT : Type(Int::class, Int.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Int {
                return buffer.int
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Int
                buffer.putInt(data)
            }
        }

        object FLOAT : Type(Float::class, Int.SIZE_BYTES) {        // Kludge - Kotlin does not yet support SIZE_BYTES for Double
            override fun get(buffer: ByteBuffer): Float {
                return buffer.float
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Float
                buffer.putFloat(data)
            }
        }

        object DOUBLE : Type(Double::class, Long.SIZE_BYTES) {        // Kludge - Kotlin does not yet support SIZE_BYTES for Double
            override fun get(buffer: ByteBuffer): Double {
                return buffer.double
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Double
                buffer.putDouble(data)
            }
        }

        object INSTANT : Type(Instant::class, Long.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Instant {
                return Instant.ofEpochMilli(buffer.long)
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Instant
                buffer.putLong(data.toEpochMilli())
            }

        }

        object STRING : Type(String::class) {
            override fun get(buffer: ByteBuffer): String {
                return buffer.getString()
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as String
                buffer.putString(data)
            }

            override fun getBytes(obj: Any, field: KProperty1<Any, Any?>): Int {
                val data = field.get(obj) as String
                return Int.SIZE_BYTES + data.toByteArray().size
            }
        }

        object LONG : Type(Long::class, Long.SIZE_BYTES) {
            override fun get(buffer: ByteBuffer): Long {
                return buffer.long
            }

            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Long
                buffer.putLong(data)
            }
        }

        object OBJECT : Type(Any::class) {
            override fun put(buffer: ByteBuffer, obj: Any, field: KProperty1<Any, Any?>) {
                val data = field.get(obj) as Any
                packInto(data, buffer)
            }

            override fun get(buffer: ByteBuffer): Any {
                TODO("not implemented")
            }

            fun <T : Any> get(clazz: KClass<out T>, buffer: ByteBuffer): T {
                return unpack(buffer, clazz)
            }

            override fun getBytes(obj: Any, field: KProperty1<Any, Any?>): Int {
                val data = field.get(obj) as Any
                return objectSize(data)
            }
        }
    }

    /**
     * A map of the supported classes to the enum objects that handle them
     */
    private val clazzes = BufferSerialisation.Type::class.sealedSubclasses.map { it.objectInstance }.filterNotNull().map {
        it.clazz.createType() to it
    }.toMap()

    private fun getMapper(parameter: KParameter): Type {
        return clazzes[parameter.type] ?: BufferSerialisation.Type.OBJECT
    }

    /**
     * Get a list of fields from the class. A field is a constructor parameter that has the same name
     * as a property. Typically these will be val/var parameters.
     */
    private fun <T : Any> getFields(clazz: KClass<T>): List<Pair<KParameter, KProperty1<Any, *>>> {
        val construct = requireNotNull(clazz.primaryConstructor)
        val parameters = construct.parameters
        val props = clazz.memberProperties as Collection<KProperty1<Any, Any>>
        return parameters.map { field -> field to props.first { prop -> prop.name == field.name } }
    }

    /**
     * Calculate the buffer size required for an object
     */
    fun <T : Any> objectSize(value: T): Int {
        val clazz = value::class
        val fields = getFields(clazz)
        return fields.sumBy {
            val entry = getMapper(it.first)
            entry.getBytes(value, it.second)
        }
    }

    /**
     * Serialise a [value] to a bytebuffer. The ordering can be specfied by the [order] argument, the
     * default is LITTLE_ENDIAN.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> pack(value: T, order: ByteOrder = ByteOrder.LITTLE_ENDIAN): ByteBuffer {
        val buffer = ByteBuffer.allocate(objectSize(value)).order(order)
        packInto(value, buffer)
        buffer.flip()
        return buffer
    }

    /** serialise into an existing buffer
     *
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> packInto(value: T, buffer: ByteBuffer): ByteBuffer {
        val clazz = value::class
        val fields = getFields(clazz)
        fields.forEach { getMapper(it.first).put(buffer, value, it.second as KProperty1<Any, Any?>) }
        return buffer
    }

    /**
     * Deserialise an object of type [clazz] from the [buffer].
     */

    fun <T : Any> unpack(buffer: ByteBuffer, clazz: KClass<out T>): T {
        val construct = requireNotNull(clazz.primaryConstructor)
        val fields = getFields(clazz).map { it.first }
        val args = fields.asSequence()
                .takeWhile { buffer.hasRemaining() }
                .map { kPar ->
                    val mapper = getMapper(kPar)
                    kPar to when (mapper) {
                        BufferSerialisation.Type.OBJECT -> {
                            BufferSerialisation.Type.OBJECT.get(kPar.type.classifier as KClass<*>, buffer)
                        }
                        else -> mapper.get(buffer)
                    }
                }
                .toList()
                .toMap()
        return construct.callBy(args)
    }

    /**
     * Deserialise an object of type [clazz] from the [array].
     * The [order] specifies the byte ordering of the array, default is LITTLE_ENDIAN.
     */

    fun <T : Any> unpack(array: ByteArray, clazz: KClass<out T>, order: ByteOrder = ByteOrder.LITTLE_ENDIAN): T {
        return unpack(ByteBuffer.wrap(array).order(order), clazz)
    }

}

fun ByteBuffer.getString(): String {
    val len = int
    val arr = ByteArray(len)
    get(arr)
    return arr.trimString()
}

fun ByteBuffer.putString(s: String): ByteBuffer {
    val bytes = s.toByteArray()
    putInt(bytes.size)
    put(bytes)
    return this
}

fun ByteArray.trimString(): String {
    for (i in 0 until size)
        if (this[i] == 0.toByte())
            return String(this, 0, i)
    return String(this)
}

fun ByteBuffer.getString(len: Int): String {
    val arr = ByteArray(len)
    get(arr)
    return arr.trimString()
}

