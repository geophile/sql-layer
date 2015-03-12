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
package com.foundationdb.server.test.pt;

import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class PTBase extends ApiTestBase {

    protected PTBase() {
        super("PT");
        tapsRegexes = new ArrayList<>();
    }
    
    protected void registerTaps() {
        // none by default
    }
    
    protected void beforeProfiling() throws Exception {
        // none by default
    }
    
    protected void afterProfileReporting() throws Exception {
        // none by default
    }
    
    protected static void log(String format, Object... args) {
        System.out.println(String.format(format, args));
    }
    
    protected String paramName() {
        return "";
    }
    
    @Before
    public void setUpProfiling() throws Exception {
        beforeProfiling();
        tapsRegexes.clear();
        registerTaps();
        Tap.setEnabled(".*", true);
        Tap.reset(".*");
        Tap.defaultToOn(true);
        testStartMS = System.currentTimeMillis();
    }

    @BeforeClass
    public static void createReportHeader() {
        log("params\ttest name\ttap name\tin\tout\ttime (ns)");
    }

    @After
    public void reportProfiling() throws Exception {
        testEndMS = System.currentTimeMillis();
        log("Total elapsed: %dms", testEndMS - testStartMS);
        // have to filter specifically, otherwise we'd have multiple TapReport[]s that we'd need to merge
        TapReport[] reportsArray = Tap.getReport(".*");
        List<TapReport> reports = new ArrayList<>(reportsArray.length);
        for (TapReport report : reportsArray) {
            String name = report.getName();
            boolean include = false;
            for (String regex : tapsRegexes) {
                if (name.matches(regex)) {
                    include = true;
                    break;
                }
            }
            if (include)
                reports.add(report);
        }
        Collections.sort(reports, REPORTS_BY_NAME);
        if (!reports.isEmpty()) {
            for (TapReport report : reports) {
                log("%s\t%s\t%s\t%d\t%d\t%d",
                        paramName().replace('\t', '_'),
                        testName(),
                        report.getName(),
                        report.getInCount(),
                        report.getOutCount(),
                        report.getCumulativeTime()
                );
            }
        }
        log("");
        afterProfileReporting();
    }

    protected final List<String> tapsRegexes;
    protected long testStartMS;
    protected long testEndMS;

    private static final Comparator<TapReport> REPORTS_BY_NAME = new Comparator<TapReport>() {
        @Override
        public int compare(TapReport o1, TapReport o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
}
