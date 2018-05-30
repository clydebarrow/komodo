package com.controlj.komodo

import org.h2.mvstore.MVMap
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Copyright (C) Control-J Pty. Ltd. ACN 103594190
 * All rights reserved
 *
 * User: clyde
 * Date: 29/5/18
 * Time: 12:18
 */
class KomodoTest {

    /**
     * Test that we can create a database and access the underlying MVStore
     */
    @Test
    fun komodoTest() {
        val komodo = Komodo()
        rwTest(komodo)
        val diskKomodo = Komodo(filename = "test.db", compressed = true, encryptionKey = "lalalala")
        rwTest(diskKomodo)
    }

    fun rwTest(komodo: Komodo) {
        val map: MVMap<Long, String> = komodo.store.openMap("test")
        map[1] = "data"
        map[2] = "otherdata"
        assertEquals(map[1], "data")
        assertEquals(map[2], "otherdata")
        komodo.commit()
        komodo.close()
    }
}