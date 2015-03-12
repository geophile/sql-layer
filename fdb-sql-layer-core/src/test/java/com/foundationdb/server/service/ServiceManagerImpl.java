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
package com.foundationdb.server.service;

import java.util.concurrent.atomic.AtomicReference;

import com.foundationdb.server.error.ServiceNotStartedException;
import com.foundationdb.server.service.servicemanager.DelegatingServiceManager;
import com.foundationdb.server.service.session.Session;

public final class ServiceManagerImpl extends DelegatingServiceManager
{
    private static final AtomicReference<ServiceManager> instance = new AtomicReference<>(null);

    public static void setServiceManager(ServiceManager newInstance) {
        if (newInstance == null) {
            instance.set(null);
        } else if (!instance.compareAndSet(null, newInstance)) {
            throw new RuntimeException(
                    "Tried to install a ServiceManager, but one was already set");
        }
    }

    public ServiceManagerImpl() {}

    /**
     * Gets the active ServiceManager; you can then use the returned instance to get any service you want.
     * @return the active ServiceManager
     * @deprecated for new code, please just use dependency injection
     */
    @Deprecated
    public static ServiceManager get() {
        return installed();
    }

    /**
     * Convenience for {@code ServiceManagerImpl.get().getSessionService().createSession()}
     * @return a new Session
     */
    public static Session newSession() {
        return installed().getSessionService().createSession();
    }

    // DelegatingServiceManager override


    @Override
    protected ServiceManager delegate() {
        return installed();
    }

    // for use by this class

    private static ServiceManager installed() {
        ServiceManager sm = instance.get();
        if (sm == null) {
            throw new ServiceNotStartedException("services haven't been started");
        }
        return sm;
    }
}
