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
package com.foundationdb.server.types.service;

import com.foundationdb.server.types.service.InstanceFinder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;

class InstanceFinderBuilder implements InstanceFinder {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
        Collection<?> resultWild = instances.get(targetClass);
        return (Collection<? extends T>) resultWild;
    }

    public <T> void put(Class<T> cls, Object... objects) {
        for (Object obj : objects) {
            instances.put(cls, cls.cast(obj));
        }
    }

    private Multimap<Class<?>,Object> instances = HashMultimap.create();
}
