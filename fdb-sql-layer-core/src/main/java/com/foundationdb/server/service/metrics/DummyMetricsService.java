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

import com.foundationdb.server.service.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** In-memory metric implementation when no durable alternative available. */
public class DummyMetricsService implements MetricsService, Service
{
    private final ConcurrentHashMap<String,BaseMetricImpl<?>> metrics = new ConcurrentHashMap<>();

    static abstract class BaseMetricImpl<T> implements BaseMetric<T> {
        private final String name;
        
        protected BaseMetricImpl(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public String toString() {
            return name + " = " + getObject();
        }
    }

    static class BooleanMetricImpl extends BaseMetricImpl<Boolean> implements BooleanMetric {
        private final AtomicBoolean bvalue = new AtomicBoolean();

        public BooleanMetricImpl(String name) {
            super(name);
        }

        @Override
        public boolean get() {
            return bvalue.get();
        }

        @Override
        public void set(boolean value) {
            bvalue.set(value);
        }
        
        @Override
        public Boolean getObject() {
            return get();
        }

        @Override
        public void setObject(Boolean value) {
            set(value);
        }
        
        @Override
        public boolean toggle() {
            while (true) {
                boolean value = bvalue.get();
                boolean newValue = !value;
                if (bvalue.compareAndSet(value, newValue)) {
                    return newValue;
                }
            }
        }
    }

    static class LongMetricImpl extends BaseMetricImpl<Long> implements LongMetric {
        private final AtomicLong lvalue = new AtomicLong();

        public LongMetricImpl(String name) {
            super(name);
        }

        @Override
        public long get() {
            return lvalue.get();
        }

        @Override
        public void set(long value) {
            lvalue.set(value);
        }
        
        @Override
        public Long getObject() {
            return get();
        }

        @Override
        public void setObject(Long value) {
            set(value);
        }
        
        @Override
        public long increment() {
            return lvalue.incrementAndGet();
        }
        
        @Override
        public long increment(long amount) {
            return lvalue.addAndGet(amount);
        }
    }

    /* MetricCollection */

    @Override
    public BooleanMetric addBooleanMetric(String name) {
        BooleanMetricImpl metric = new BooleanMetricImpl(name);
        addMetric(metric);
        return metric;
    }

    @Override
    public LongMetric addLongMetric(String name) {
        LongMetricImpl metric = new LongMetricImpl(name);
        return metric;
    }

    @Override
    public <T> void removeMetric(BaseMetric<T> metric) {
        metrics.remove(metric.getName(), metric);
    }
    
    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

    /* Internal */

    protected void addMetric(BaseMetricImpl<?> metric) {
        if (metrics.putIfAbsent(metric.getName(), metric) != null) {
            throw new IllegalArgumentException("There is already a metric named " + metric.getName());
        }
    }
}
