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
package com.foundationdb.server.service.metrics;

import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.test.it.FDBITBase;

import com.foundationdb.Transaction;
import com.foundationdb.async.Function;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class FDBMetricsServiceIT extends FDBITBase
{
    private FDBHolder fdbService;
    private FDBMetricsService metricsService;

    @Before
    public void wipeOutOld() {
        metricsService = fdbMetricsService();
        fdbService = fdbHolder();

        metricsService.completeBackgroundWork();
        fdbService.getTransactionContext().run(new Function<Transaction,Void>() {
                                         @Override
                                         public Void apply(Transaction tr) {
                                             tr.options().setAccessSystemKeys();
                                             metricsService.deleteBooleanMetric(tr, "TestBoolean");
                                             metricsService.deleteLongMetric(tr, "TestLong");
                                             return null;
                                         }
                                     });
        metricsService.reset();
    }

    @Test
    public void saveEnabled() {
        BooleanMetric testBoolean = metricsService.addBooleanMetric("TestBoolean");
        LongMetric testLong = metricsService.addLongMetric("TestLong");
        assertFalse(testBoolean.isEnabled());
        assertFalse(testLong.isEnabled());
        ((FDBMetric<Boolean>)testBoolean).setEnabled(true);
        ((FDBMetric<Long>)testLong).setEnabled(true);
        metricsService.completeBackgroundWork();
        metricsService.removeMetric(testBoolean);
        metricsService.removeMetric(testLong);
        testBoolean = metricsService.addBooleanMetric("TestBoolean");
        testLong = metricsService.addLongMetric("TestLong");
        assertTrue(testBoolean.isEnabled());
        assertTrue(testLong.isEnabled());
    }

    static class TestValues {
        List<FDBMetric.Value<Boolean>> booleanValues;
        List<FDBMetric.Value<Long>> longValues;
    }

    @Test
    public void saveMetrics() {
        BooleanMetric testBoolean = metricsService.addBooleanMetric("TestBoolean");
        LongMetric testLong = metricsService.addLongMetric("TestLong");
        ((FDBMetric<Boolean>)testBoolean).setEnabled(true);
        ((FDBMetric<Long>)testLong).setEnabled(true);
        testBoolean.set(false);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testBoolean.set(true);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testBoolean.toggle();
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.set(100);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.increment();
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.increment(-1);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        metricsService.completeBackgroundWork();
        final FDBMetric<Boolean> m1 = (FDBMetric<Boolean>)testBoolean;
        final FDBMetric<Long> m2 = (FDBMetric<Long>)testLong;
        TestValues values = fdbService.getTransactionContext()
            .run(new Function<Transaction,TestValues> () {
                     @Override
                     public TestValues apply(Transaction tr) {
                         tr.options().setAccessSystemKeys();
                         TestValues values = new TestValues();
                         values.booleanValues = m1.readAllValues(tr).get();
                         values.longValues = m2.readAllValues(tr).get();
                         return values;
                     }
                 });
        checkValues(values.booleanValues, false, true, false);
        checkValues(values.longValues, 0L, 100L, 101L, 100L);
    }

    private <T> void checkValues(List<FDBMetric.Value<T>> values,
                                 Object... expected) {
        assertEquals("number of values", expected.length, values.size());
        long maxTime = System.currentTimeMillis() * 1000000;
        long minTime = maxTime - 5 * 1000 * 1000000;
        for (int i = 0; i < expected.length; i++) {
            FDBMetric.Value<T> value = values.get(i);
            assertEquals("value " + i + " of " + values, expected[i], value.value);
            assertTrue("time in range", (minTime < value.time) && (value.time < maxTime));
            minTime = value.time;
        }
    }
}
