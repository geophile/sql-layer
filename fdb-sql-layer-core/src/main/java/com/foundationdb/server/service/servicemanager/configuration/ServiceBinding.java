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
package com.foundationdb.server.service.servicemanager.configuration;

import com.foundationdb.util.ArgumentValidation;

public final class ServiceBinding {

    // DefaultServiceBinding interface

    public boolean isLocked() {
        return locked;
    }

    public void lock() {
        locked = true;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        if (classLoader != null)
            this.classLoader = classLoader;
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    public void setImplementingClass(String className) {
        if (isLocked()) {
            throw new ServiceConfigurationException("can't set new implementing class: " + interfaceName + " is locked");
        }
        implementingClassName = className;
    }

    public String getImplementingClassName() {
        return implementingClassName;
    }

    public boolean isDirectlyRequired() {
        return directlyRequired;
    }

    public void markDirectlyRequired() {
        directlyRequired = true;
    }

    // Object interface

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ServiceBinding)) return false;

        ServiceBinding that = (ServiceBinding) o;

        return !(interfaceName != null ? !interfaceName.equals(that.interfaceName) : that.interfaceName != null);

    }

    @Override
    public final int hashCode() {
        return interfaceName != null ? interfaceName.hashCode() : 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ServiceBinding(");
        builder.append(getInterfaceName()).append(" -> ").append(getImplementingClassName());
        builder.append(
                isLocked() ? ", locked)" : ", unlocked)"
        );
        return builder.toString();
    }

    // private methods

    // DefaultLockableServiceBinding interface

    public ServiceBinding(String interfaceName) {
        ArgumentValidation.notNull("interface name", interfaceName);
        this.interfaceName = interfaceName;
    }

    // object state

    private final String interfaceName;
    private String implementingClassName;
    private boolean locked;
    private boolean directlyRequired;
    private ClassLoader classLoader = ServiceBinding.class.getClassLoader();
}
