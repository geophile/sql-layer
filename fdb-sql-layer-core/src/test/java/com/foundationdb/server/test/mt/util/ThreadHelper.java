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
package com.foundationdb.server.test.mt.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class ThreadHelper
{
    private static final long DEFAULT_TIMEOUT_MILLIS = 5 * 1000;

    public static class UncaughtHandler implements Thread.UncaughtExceptionHandler {
        public final Map<Thread, Throwable> thrown = Collections.synchronizedMap(new HashMap<Thread, Throwable>());

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            thrown.put(t, e);
        }

        @Override
        public String toString() {
            Map<String,String> nameToError = new TreeMap<>();
            for(Entry<Thread, Throwable> entry : thrown.entrySet()) {
                nameToError.put(entry.getKey().getName(), entry.getValue().getMessage());
            }
            return nameToError.toString();
        }
    }


    public static UncaughtHandler start(Thread... threads) {
        return start(Arrays.asList(threads));
    }

    public static void join(UncaughtHandler handler, long timeoutMillis, Thread... threads) {
        join(handler, timeoutMillis, Arrays.asList(threads));
    }

    public static UncaughtHandler startAndJoin(long timeoutMillis, Thread... threads) {
        return startAndJoin(timeoutMillis, Arrays.asList(threads));
    }
    public static void runAndCheck(Thread... threads) {
        runAndCheck(Arrays.asList(threads));
    }

    public static void runAndCheck(long timeoutMillis, Thread... threads) {
        runAndCheck(timeoutMillis, Arrays.asList(threads));
    }


    public static UncaughtHandler start(Collection<? extends Thread> threads) {
        UncaughtHandler handler = new UncaughtHandler();
        for(Thread t : threads) {
            t.setUncaughtExceptionHandler(handler);
            t.start();
        }
        return handler;
    }

    public static void join(UncaughtHandler handler, long timeoutMillis, Collection<? extends Thread> threads) {
        for(Thread t : threads) {
            Throwable error = null;
            try {
                t.join(timeoutMillis);
                if(t.isAlive()) {
                    error = new AssertionError("Thread did not complete in timeout: " + timeoutMillis);
                    t.interrupt();
                }
            } catch(InterruptedException e) {
                error = e;
            }
            if(error != null) {
                handler.uncaughtException(t, error);
            }
        }
    }

    public static UncaughtHandler startAndJoin(Collection<? extends Thread> threads) {
        return startAndJoin(DEFAULT_TIMEOUT_MILLIS, threads);
    }

    public static UncaughtHandler startAndJoin(long timeoutMillis, Collection<? extends Thread> threads) {
        UncaughtHandler handler = start(threads);
        join(handler, timeoutMillis, threads);
        return handler;
    }

    public static void runAndCheck(Collection<? extends Thread> threads) {
        runAndCheck(DEFAULT_TIMEOUT_MILLIS, threads);
    }

    public static void runAndCheck(long timeoutMillis, Collection<? extends Thread> threads) {
        UncaughtHandler handler = startAndJoin(timeoutMillis, threads);
        assertEquals("Thread errors", new UncaughtHandler().toString(), handler.toString());
    }
}
