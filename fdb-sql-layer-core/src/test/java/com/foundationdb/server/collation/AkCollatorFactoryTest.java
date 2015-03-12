/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.server.collation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import com.foundationdb.server.error.UnsupportedCollationException;

import org.junit.Test;

import com.ibm.icu.text.Collator;

public class AkCollatorFactoryTest {

    private final static int NTHREADS = 10;

    private AkCollatorFactory.Mode DEFAULT_MODE = AkCollatorFactory.Mode.STRICT;

    @Test
    public void uniquePerThread() throws Exception {
        final AtomicInteger threadIndex = new AtomicInteger();
        final Collator[] array = new Collator[NTHREADS];
        Thread[] threads = new Thread[NTHREADS];
        for (int i = 0; i < NTHREADS; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    int index = threadIndex.getAndIncrement();
                    AkCollatorICU icu = (AkCollatorICU) (AkCollatorFactory.getAkCollator("sv_se_ci"));
                    array[index] = icu.collator.get();
                }
            });
        }
        for (int i = 0; i < NTHREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NTHREADS; i++) {
            threads[i].join();
        }
        for (int i = 0; i < NTHREADS; i++) {
            assertNotNull("Null", array[i]);
            for (int j = 0; j < i; j++) {
                assertTrue("Not unique", array[i] != array[j]);
            }
        }
    }

    @Test
    public void makeMySQLCollator() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(DEFAULT_MODE);
            final AkCollator collator = AkCollatorFactory.getAkCollator("sv_se_ci");
            assertEquals("Collector should have correct name", "sv_se_ci_co", collator.getScheme());
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test
    public void collatorById() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(DEFAULT_MODE);
            AkCollator collator = AkCollatorFactory.getAkCollator(0);
            assertEquals("Should be the AkCollatorBinary singleton",
                    AkCollatorFactory.UCS_BINARY_COLLATOR, collator);
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void collationBadMode() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(DEFAULT_MODE);
            AkCollatorFactory.setCollationMode("Invalid");
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test(expected = UnsupportedCollationException.class)
    public void collationBadName() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode("Strict");
            AkCollatorFactory.getAkCollator("fricostatic_sengalese_ci");
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test(expected = UnsupportedCollationException.class)
    public void collationBadScheme() throws Exception {
        CollationSpecifier collationSpecifier = new CollationSpecifier("en_us_bad_arguments");
        AkCollatorFactory.forScheme(collationSpecifier);
    }

    @Test
    public void collationLooseMode() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode("LOOSE");
            assertEquals("Should be binary", AkCollatorFactory.UCS_BINARY_COLLATOR, AkCollatorFactory
                    .getAkCollator("fricostatic_sengalese_ci"));
            assertEquals("Collector should have correct name", "en_ie_ci_cx", AkCollatorFactory.getAkCollator(
                    "en_ie_ci_cx").getScheme());

        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test
    public void collationDisabledMode() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode("Disabled");
            assertEquals("Should be binary", AkCollatorFactory.UCS_BINARY_COLLATOR, AkCollatorFactory
                    .getAkCollator("latin1_swedish_ci"));
            assertEquals("Should be binary", AkCollatorFactory.UCS_BINARY_COLLATOR, AkCollatorFactory
                    .getAkCollator("invalid_collation_name"));
            assertEquals("Should be binary", AkCollatorFactory.UCS_BINARY_COLLATOR, AkCollatorFactory
                    .getAkCollator("en_us_ci"));
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }

    @Test
    public void fromCache() throws Exception {
        AkCollatorFactory.Mode saveMode = AkCollatorFactory.getCollationMode();
        try {
            AkCollatorFactory.setCollationMode(DEFAULT_MODE);
            AkCollator c = AkCollatorFactory.getAkCollator("sv_se_ci");
            int cid = c.getCollationId();
            int hits = AkCollatorFactory.getCacheHits();
            for (int i = 0; i < 10; i++) {
                c = AkCollatorFactory.getAkCollator("sv_se_ci");
            }
            assertEquals("Should have used cache", hits + 10, AkCollatorFactory.getCacheHits());

            for (int i = 0; i < 10; i++) {
                c = AkCollatorFactory.getAkCollator(cid);
            }
            assertEquals("Should have used cache", hits + 20, AkCollatorFactory.getCacheHits());
        } finally {
            AkCollatorFactory.setCollationMode(saveMode);
        }
    }
}
