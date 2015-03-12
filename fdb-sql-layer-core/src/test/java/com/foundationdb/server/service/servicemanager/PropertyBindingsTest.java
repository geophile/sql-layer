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
package com.foundationdb.server.service.servicemanager;

import com.foundationdb.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.PropertyBindings;
import com.google.inject.Module;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public final class PropertyBindingsTest {

    @Test
    public void none() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().get().loadInto(config);
        compare(config);
    }

    @Test
    public void ignored() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("foo", "bar").get().loadInto(config);
        compare(config);
    }

    @Test
    public void goodBinding() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:foo", "bar").get().loadInto(config);
        compare(config, "bind foo to bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void bindNoInterface() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:", "bar").get().loadInto(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bindEmptyImplementation() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:foo", "").get().loadInto(config);
    }

    @Test
    public void goodRequire() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "").get().loadInto(config);
        compare(config, "require foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void valuedRequire() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "bar").get().loadInto(config);
        compare(config, "require foo");
    }

    @Test
    public void prioritize() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "").add("require:bar", "").add("prioritize:foo", "").get().loadInto(config);
        compare(config, "require foo", "prioritize foo", "require bar");
    }

    private static void compare(StringsConfig actual, String... expected) {
        assertEquals("strings", Arrays.asList(expected), actual.messages());
    }

    private static class PropsBuilder {

        PropsBuilder add(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        PropertyBindings get() {
            return new PropertyBindings(properties);
        }

        private final Properties properties = new Properties();
    }

    private static class StringsConfig implements ServiceConfigurationHandler {
        @Override
        public void bind(String interfaceName, String implementingClassName, ClassLoader ignored) {
            messages.add("bind " + interfaceName + " to " + implementingClassName);
        }

        @Override
        public void require(String interfaceName) {
            messages.add("require " + interfaceName);
        }

        @Override
        public void lock(String interfaceName) {
            messages.add("lock: " + interfaceName);
        }

        @Override
        public void mustBeLocked(String interfaceName) {
            messages.add("must be locked: " + interfaceName);
        }

        @Override
        public void mustBeBound(String interfaceName) {
            messages.add("must be bound: " + interfaceName);
        }

        @Override
        public void prioritize(String interfaceName) {
            messages.add("prioritize " + interfaceName);
        }

        @Override
        public void sectionEnd() {
            messages.add("section end");
        }

        @Override
        public void unrecognizedCommand(String where, Object command, String message) {
            messages.add("unrecognized command");
        }

        @Override
        public void bindModules(List<Module> modules) {
            for (Module module : modules)
                messages.add("binding module " + module.getClass());
        }

        @Override
        public void bindModulesError(String where, Object command, String message) {
            messages().add("bind-modules error");
        }

        @Override
        public void unbind(String interfaceName) {
            messages.add("unbind " + interfaceName);
        }

        public List<String> messages() {
            return messages;
        }

        private final List<String> messages = new ArrayList<>();
    }
}
