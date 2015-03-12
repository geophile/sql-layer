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
package com.foundationdb.server.service.listener;

import com.foundationdb.server.service.Service;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class ListenerServiceImpl implements ListenerService, Service
{
    private final Set<TableListener> tableListeners;
    private final Set<RowListener> rowListeners;

    public ListenerServiceImpl() {
        // Modifications are only expected at startup and shutdown, but be prepared for dynamic services.
        this.tableListeners = new CopyOnWriteArraySet<>();
        this.rowListeners = new CopyOnWriteArraySet<>();
    }


    //
    // ListenerService
    //

    @Override
    public Iterable<TableListener> getTableListeners() {
        return tableListeners;
    }

    @Override
    public void registerTableListener(TableListener listener) {
        tableListeners.add(listener);
    }

    @Override
    public void deregisterTableListener(TableListener listener) {
        tableListeners.remove(listener);
    }

    @Override
    public Iterable<RowListener> getRowListeners() {
        return rowListeners;
    }

    @Override
    public void registerRowListener(RowListener listener) {
        rowListeners.add(listener);
    }

    @Override
    public void deregisterRowListener(RowListener listener) {
        rowListeners.remove(listener);
    }


    //
    // Service
    //

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        tableListeners.clear();
        rowListeners.clear();
    }

    @Override
    public void crash() {
        stop();
    }
}
