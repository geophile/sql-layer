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
package com.foundationdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

public class GCMonitor extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(GCMonitor.class);
    private static final String THREAD_NAME = "GC_MONITOR";
    private static final String LOG_MSG_FORMAT = "%s spent %s ms on %s collections, heap at %.2f%%";

    private static class GCInfo {
        public final GarbageCollectorMXBean mxBean;
        public long lastCollectionCount = 0;
        public long lastCollectionTime = 0;

        public GCInfo(GarbageCollectorMXBean mxBean) {
            this.mxBean = mxBean;
        }

        public long updateCollectionTime() {
            long prev = lastCollectionTime;
            lastCollectionTime = mxBean.getCollectionTime();
            return lastCollectionTime - prev;
        }

        public long updateCollectionCount() {
            long prev = lastCollectionCount;
            lastCollectionCount = mxBean.getCollectionCount();
            return lastCollectionCount - prev;
        }
    }

    private final int interval;
    private final int logThreshold;
    private final List<GCInfo> gcInfo = new ArrayList<>();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private volatile boolean running;


    public GCMonitor(int interval, int logThreshold) {
        super(THREAD_NAME);
        this.interval = interval;
        this.logThreshold = logThreshold;
        for(GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcInfo.add(new GCInfo(gcBean));
        }
    }

    @Override
    public void run() {
        running = !gcInfo.isEmpty();
        while(running) {
            runInternal();
            try {
                Thread.sleep(interval);
            } catch(InterruptedException e) {
                LOG.debug("Interrupted, exiting", e);
                running = false;
            }
        }
    }

    public void stopRunning() {
        this.running = false;
        interrupt();
    }

    private void runInternal() {
        for(GCInfo info : gcInfo) {
            long elapsed = info.updateCollectionTime();
            long collections = info.updateCollectionCount();

            // Skip if there were no collections or very few (so as to not log on every startup)
            if(collections == 0 || info.lastCollectionCount <= 3) {
                continue;
            }

            long timePerCollection = elapsed / collections;
            boolean doWarn = (timePerCollection >= logThreshold);
            boolean doDebug = LOG.isDebugEnabled();

            if(doWarn || doDebug) {
                MemoryUsage usage = memoryBean.getHeapMemoryUsage();
                double percent = 100 * ((double)usage.getUsed() / (double)usage.getMax());
                String message =  String.format(LOG_MSG_FORMAT, info.mxBean.getName(), elapsed, collections, percent);
                if(doWarn) {
                    LOG.warn(message);
                } else {
                    LOG.debug(message);
                }
            }

            // Anything we can do to try and reduce pressure?
        }
    }
}
