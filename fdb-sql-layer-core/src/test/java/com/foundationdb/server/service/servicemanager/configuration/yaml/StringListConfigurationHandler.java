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
package com.foundationdb.server.service.servicemanager.configuration.yaml;

import com.foundationdb.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class StringListConfigurationHandler implements ServiceConfigurationHandler {

    // YamlConfigurationStrategy interface

    @Override
    public void bind(String interfaceName, String implementingClassName, ClassLoader ignored) {
        say("BIND %s -> %s", interfaceName, implementingClassName);
    }

    @Override
    public void bindModules(List<Module> modules) {
        for (Module module : modules)
            say("BIND-MODULE %s", module.getClass());
    }

    @Override
    public void lock(String interfaceName) {
        say("LOCK %s", interfaceName);
    }

    @Override
    public void require(String interfaceName) {
        say("REQUIRE %s", interfaceName);
    }

    @Override
    public void mustBeLocked(String interfaceName) {
        say("MUST BE LOCKED %s", interfaceName);
    }

    @Override
    public void mustBeBound(String interfaceName) {
        say("MUST BE BOUND %s", interfaceName);
    }

    @Override
    public void prioritize(String interfaceName) {
        say("PRIORITIZE %s", interfaceName);
    }

    @Override
    public void sectionEnd() {
        say("SECTION END");
    }

    @Override
    public void unrecognizedCommand(String where, Object command, String message) {
        say("ERROR: %s (at %s) %s", message, where, command);
    }

    @Override
    public void bindModulesError(String where, Object command, String message) {
        say("BIND-MODULES ERROR: %s (at %s) %s", message, where, command);
    }

    @Override
    public void unbind(String interfaceName) {
        say("UNBIND %s", interfaceName);
    }

    // StringListStrategy interface

    public List<String> strings() {
        return unmodifiableStrings;
    }

    // private methods
    private void say(String format, Object... args) {
        strings.add(String.format(format, args));
    }

    // object state
    private final List<String> strings = new ArrayList<>();
    private final List<String> unmodifiableStrings = Collections.unmodifiableList(strings);
}
