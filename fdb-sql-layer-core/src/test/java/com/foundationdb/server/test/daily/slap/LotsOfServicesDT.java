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
package com.foundationdb.server.test.daily.slap;

import com.foundationdb.server.test.daily.DailyBase;
import org.junit.Test;

public final class LotsOfServicesDT extends DailyBase {
    @Test
    public void loop() throws Throwable {

        stopTestServices(); // shut down ApiTestBase's @Before services

        final int LOOP_COUNT = 1000;
        int i=0;
        try {
            for (; i < LOOP_COUNT; ++i) {
                startTestServices();
                Thread.sleep(10);
                stopTestServices();
            }
        } catch (Throwable e) {
            throw new RuntimeException("At i="+i, e);
        }

        startTestServices(); // so that ApiTestBase's @After has something to shut down
    }
}
