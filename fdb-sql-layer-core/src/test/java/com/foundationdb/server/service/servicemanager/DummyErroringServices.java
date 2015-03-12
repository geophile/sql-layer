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
package com.foundationdb.server.service.servicemanager;

/**
 * Alpha <- Beta <- Gamma <- nothing
 */
final class DummyErroringServices {

    public static class ErroringAlpha implements DummyInterfaces.Alpha {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Beta.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
        }

        @Override
        public void stop() {}
    }

    public static class ErroringBeta implements DummyInterfaces.Beta {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Gamma.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
            throw new ErroringException();
        }

        @Override
        public void stop() {}
    }

    public static class ErroringGamma implements DummyInterfaces.Gamma {
        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    static class ErroringException extends RuntimeException {}
}
