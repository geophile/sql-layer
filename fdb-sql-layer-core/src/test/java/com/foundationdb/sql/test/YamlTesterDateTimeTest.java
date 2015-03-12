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
package com.foundationdb.sql.test;

import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.TimeZone;

import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.foundationdb.sql.test.YamlTester.DateTimeChecker;
import com.foundationdb.sql.test.YamlTester.TimeChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests the !date, !time underpinnings to insure they are accurate
 */
public class YamlTesterDateTimeTest {
    private static final Logger LOG = LoggerFactory.getLogger(YamlTesterDateTimeTest.class);

    private static final DateTimeZone UTC = DateTimeZone.getProvider().getZone("UTC");
    
    @Test
    public void testTimeTag() {
        Calendar cal = getCalendar();
        String time = String.format("%02d:%02d:%02d",
                cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND));
        test(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, -30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        test(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, 30);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        test(time);
    }

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance(UTC.toTimeZone());
        cal.setTimeInMillis(System.currentTimeMillis());
        return cal;
    }

    @Test
    public void testTimeTag_Negative() {
        Calendar cal = getCalendar();
        cal.roll(Calendar.MINUTE, 5);
        String time = String.format("%02d:%02d:%02d",
                cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, 2);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, -1);
        time = String.format("%02d:%02d:%02d", cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
        testFail(time);
    }

    @Test
    public void testDateTimeTag() {
        Calendar cal = getCalendar();
        String time = formatDateTime(cal);
        testdt(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, -30);
        time = formatDateTime(cal);
        testdt(time);
        cal = getCalendar();
        cal.roll(Calendar.SECOND, 30);
        time = formatDateTime(cal);
        testdt(time);
    }

    private String formatDateTime(Calendar cal) {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%03d",
                cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
                cal.get(Calendar.MILLISECOND));
    }

    @Test
    public void testDateTimeTag_Negative() {
        Calendar cal = getCalendar();
        cal.roll(Calendar.MINUTE, 5);
        String time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.HOUR_OF_DAY, 1);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, 2);
        time = formatDateTime(cal);
        testdtFail(time);
        cal = getCalendar();
        cal.roll(Calendar.MINUTE, -1);
        time = formatDateTime(cal);
        testdtFail(time);
    }

    private static void test(String output) {
        boolean result = new TimeChecker().compareExpected(output);
        if (!result) {
            fail("Time check failed with " + output);
        }
    }

    private static void testFail(String output) {
        boolean result = new TimeChecker().compareExpected(output);
        if (result) {
            fail("Time check failed with " + output);
        }
    }

    private static void testdt(String output) {
        boolean result = new DateTimeChecker().compareExpected(output);
        if (!result) {
            fail("Time check failed with " + output);
        } else {
            LOG.debug(output);
        }
    }

    private static void testdtFail(String output) {
        boolean result = new DateTimeChecker().compareExpected(output);
        if (result) {
            fail("Time check failed with " + output);
        } else {
            LOG.debug(output);
        }
    }

}
