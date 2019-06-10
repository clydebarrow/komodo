package com.controlj.komodo

import com.controlj.komodo.BufferSerialisation.pack
import com.controlj.komodo.BufferSerialisation.unpack
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test
import org.threeten.bp.Instant
import java.nio.ByteOrder

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 * All rights reserved
 *
 *
 * User: clyde
 * Date: 2019-03-21
 * Time: 11:15
 */
class BufferSerialisationTest {

    private val unicode = "unicode string ʼʽ"
    data class ShortData(val bval1: Byte, val intVal: Int, val lval: Long, val bval2: Byte, val short: Short)

    data class TestData(val bval1: Byte, val intVal: Int, val lval: Long, val bval2: Byte, val short: Short,
                        val fval: Float, val dval: Double, val boolval: Boolean, val instant: Instant,
                        val string: String, val char: Char)

    private val t = TestData(1, 2, 3, 4, 5, 123.4f, 123.45, true, Instant.ofEpochMilli(1000000L), unicode, 0x1234.toChar())

    data class Inner(val intval: Int, val dval: Double)
    data class Outer(val intval: Int, val inner: Inner, val string: String)

    /**
     * Test for serialisation of an object
     */
    @Test
    fun testSerialise() {
        var b = pack(t)
        assertEquals(ByteOrder.LITTLE_ENDIAN, b.order())
        //println("buffer size = ${b.remaining()}")
        assertEquals(1.toByte(), b.get())
        assertEquals(2, b.int)
        assertEquals(3L, b.long)
        assertEquals(4.toByte(), b.get())
        assertEquals(5.toShort(), b.short)
        assertEquals(123.4f, b.float)
        assertEquals(123.45, b.double, .00001)
        assertEquals(1.toByte(), b.get())
        assertEquals(1000000L, b.long)
        assertEquals(19, b.int)
        assertEquals(unicode, b.getString(19))
        assertEquals(0x1234.toShort(), b.short)
        assertEquals(0, b.remaining())

        // check that it works with big-endian
        b = pack(t, ByteOrder.BIG_ENDIAN)
        assertEquals(ByteOrder.BIG_ENDIAN, b.order())
        //println("buffer size = ${b.remaining()}")
        assertEquals(1.toByte(), b.get())
        assertEquals(2, b.int)
        assertEquals(3L, b.long)
        assertEquals(4.toByte(), b.get())
        assertEquals(5.toShort(), b.short)
        assertEquals(123.4f, b.float)
        assertEquals(123.45, b.double, .00001)
        assertEquals(1.toByte(), b.get())
        assertEquals(1000000L, b.long)
        assertEquals(19, b.int)
        assertEquals(unicode, b.getString(19))
        assertEquals(0x1234.toShort(), b.short)
        assertEquals(0, b.remaining())
    }

    /**
     * Test deserialisation
     */

    @Test
    fun testUnpack() {
        val b = pack(t)
        assertEquals(t, unpack(b, TestData::class))
    }

    /**
     * Test unpacking from a byte array
     */
    @Test
    fun testArrayUnpack() {
        var b = byteArrayOf(1, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 5, 0)
        var o = unpack(b, ShortData::class)
        assertEquals(1.toByte(), o.bval1)
        assertEquals(2, o.intVal)
        assertEquals(3L, o.lval)
        assertEquals(4.toByte(), o.bval2)
        assertEquals(5.toShort(), o.short)
        b = byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF)
        o = unpack(b, ShortData::class, ByteOrder.BIG_ENDIAN)
        assertEquals(0.toByte(), o.bval1)
        assertEquals(0x01020304, o.intVal)
        assertEquals(0x5060708090A0B0CL, o.lval)
        assertEquals(0xD.toByte(), o.bval2)
        assertEquals(0xE0F.toShort(), o.short)
    }

    data class WithOpts(val a: Int, val b: Int = 1, val c: Long = 30L)

    /**
     * Test deserialisation from a short buffer
     */
    @Test
    fun testOpts() {
        val o = unpack(byteArrayOf(10, 0, 0, 0, 11, 0, 0, 0), WithOpts::class)
        assertEquals(10, o.a)
        assertEquals(11, o.b)
        assertEquals(30L, o.c)
    }

    @Test
    fun testObject() {
        val inner = Inner(123, 456.0)
        val outer = Outer(789, inner, "abcdef")
        val buffer = pack(outer)
        val created = unpack(buffer, Outer::class)
        assertEquals(outer, created)
        assertFalse(outer === created)
    }
}