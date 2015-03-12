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

import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class DefaultServiceConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName, ClassLoader classLoader) {
        builder.bind(interfaceName, implementingClassName, classLoader);
    }

    @Override
    public void bindModules(List<Module> modules) {
        if (this.modules == null)
            this.modules = new ArrayList<>(modules.size());
        this.modules.addAll(modules);
    }

    @Override
    public void lock(String interfaceName) {
        builder.lock(interfaceName);
    }

    @Override
    public void require(String interfaceName) {
        builder.markDirectlyRequired(interfaceName);
    }

    @Override
    public void mustBeLocked(String interfaceName) {
        builder.mustBeLocked(interfaceName);
    }

    @Override
    public void mustBeBound(String interfaceName) {
        builder.mustBeBound(interfaceName);
    }

    @Override
    public void prioritize(String interfaceName) {
        builder.prioritize(interfaceName);
    }

    @Override
    public void sectionEnd() {
        builder.markSectionEnd();
    }

    @Override
    public void unrecognizedCommand(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("unrecognized command at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    @Override
    public void bindModulesError(String where, Object command, String message) {
        throw new ServiceConfigurationException(
                String.format("bind-modules error at %s: %s (%s)",
                        where,
                        message,
                        command
                )
        );
    }

    @Override
    public void unbind(String interfaceName) {
        builder.unbind(interfaceName);
    }

    // DefaultServiceConfigurationHandler interface

    public Collection<? extends Module> getModules() {
        Collection<Module> internal = modules == null ? Collections.<Module>emptyList() : modules;
        return Collections.unmodifiableCollection(internal);
    }

    public Collection<ServiceBinding> serviceBindings(boolean strict) {
        return builder.getAllBindings(strict);
    }

    public List<String> priorities() {
        return builder.getPriorities();
    }

    // object state
    private final ServiceBindingsBuilder builder = new ServiceBindingsBuilder();
    private Collection<Module> modules = null;
}
