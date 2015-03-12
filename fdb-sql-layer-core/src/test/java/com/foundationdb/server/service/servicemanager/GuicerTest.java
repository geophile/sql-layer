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

import com.foundationdb.server.service.servicemanager.configuration.ServiceBinding;
import com.foundationdb.util.Strings;
import com.google.inject.ProvisionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class GuicerTest {

    @Before
    public void clearMessages() {
        DummyInterfaces.clearMessages();
    }

    @Test
    public void simple() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummySimpleServices.SimpleAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummySimpleServices.SimpleBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        startRequiredServices(guicer);
        assertEquals(
                "messages",
                joined(
                        "starting SimpleAlpha",
                        "starting SimpleBeta",
                        "starting SimpleGamma",
                        "started SimpleGamma",
                        "started SimpleBeta",
                        "started SimpleAlpha"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    @Test
    public void mixedDI() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummyMixedDIServices.MixedDIAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummyMixedDIServices.MixedDIBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummyMixedDIServices.MixedDIGamma.class, false)
        );
        startRequiredServices(guicer);
        assertEquals(
                "messages",
                joined(
                        "starting MixedDIBeta",
                        "starting MixedDIGamma",
                        "started MixedDIGamma",
                        "started MixedDIBeta",
                        "starting MixedDIAlpha",
                        "started MixedDIAlpha"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    @Test
    public void errorOnStartup() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummyErroringServices.ErroringAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummyErroringServices.ErroringBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummyErroringServices.ErroringGamma.class, false)
        );
        try{
            startRequiredServices(guicer);
            fail("should have caught ErroringException");
        } catch (ProvisionException e) {
            assertEventualCause(e, DummyErroringServices.ErroringException.class);
            assertEquals(
                    "messages",
                    joined(
                            "starting ErroringAlpha",
                            "starting ErroringBeta",
                            "starting ErroringGamma",
                            "started ErroringGamma",
                            "stopping ErroringGamma",
                            "stopped ErroringGamma"
                    ),
                    Strings.join(DummyInterfaces.messages())
            );
        }
    }

    @Test
    public void singletonNess() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        guicer.get(DummyInterfaces.Gamma.class, MESSAGING_ACTIONS);
        guicer.get(DummyInterfaces.Gamma.class, MESSAGING_ACTIONS);
        assertEquals(
                "messages",
                joined(
                        "starting SimpleGamma",
                        "started SimpleGamma"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    @Test(expected=IllegalArgumentException.class)
    public void getRequiresInterface() throws ClassNotFoundException {
        Guicer guicer = messageGuicer(
            bind(DummyInterfaces.Alpha.class, DummySimpleServices.SimpleAlpha.class, true),
            bind(DummyInterfaces.Beta.class, DummySimpleServices.SimpleBeta.class, false),
            bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        startRequiredServices(guicer);
        Object a = guicer.get(DummyInterfaces.Alpha.class, MESSAGING_ACTIONS);
        Object b = guicer.get(DummyInterfaces.Alpha.class, MESSAGING_ACTIONS);
        assertEquals("two different gets", a, b);
        guicer.get(DummySimpleServices.SimpleAlpha.class, MESSAGING_ACTIONS);
    }

    private void startRequiredServices(Guicer guicer) {
        for (Class<?> clazz : guicer.directlyRequiredClasses()) {
            guicer.get(clazz, MESSAGING_ACTIONS);
        }
    }

    private void assertEventualCause(Throwable e, Class<? extends Throwable> exceptionClassToFind) {
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().equals(exceptionClassToFind)) {
                return;
            }
            cause = cause.getCause();
        }
        fail(exceptionClassToFind + " was not in the causes of " + e);
    }

    private static Guicer messageGuicer(ServiceBinding... bindings) throws ClassNotFoundException {
        return onlyGuicer = Guicer.forServices(Arrays.asList(bindings));
    }

    private static <T> ServiceBinding bind(Class<T> theInterface, Class<? extends T> theClass, boolean required) {
        if (!theInterface.isInterface()) {
            throw new IllegalArgumentException("theInterface must be an interface class: " + theInterface);
        }
        if (theClass.isInterface()) {
            throw new IllegalArgumentException("theClass must not be an interface class: " + theClass);
        }
        ServiceBinding binding = new ServiceBinding(theInterface.getName());
        binding.setImplementingClass(theClass.getName());
        if (required) {
            binding.markDirectlyRequired();
        }
        return binding;
    }

    private static String joined(String... strings) {
        return Strings.join(Arrays.asList(strings));
    }

    // For use within package

    static Guicer onlyGuicer() {
        if (onlyGuicer == null) {
            throw new IllegalStateException("no guicer set");
        }
        return onlyGuicer;
    }

    // class state

    static final Guicer.ServiceLifecycleActions<DummyInterfaces.DummyService> MESSAGING_ACTIONS
            = new Guicer.ServiceLifecycleActions<DummyInterfaces.DummyService>()
    {
        @Override
        public void onStart(DummyInterfaces.DummyService service) {
            DummyInterfaces.addMessage("starting " + service.getClass().getSimpleName());
            service.start();
            DummyInterfaces.addMessage("started " + service.getClass().getSimpleName());
        }

        @Override
        public void onShutdown(DummyInterfaces.DummyService service) {
            DummyInterfaces.addMessage("stopping " + service.getClass().getSimpleName());
            service.stop();
            DummyInterfaces.addMessage("stopped " + service.getClass().getSimpleName());
        }

        @Override
        public DummyInterfaces.DummyService castIfActionable(Object object) {
            return (object instanceof DummyInterfaces.DummyService) ? (DummyInterfaces.DummyService) object : null;
        }
    };

    private static Guicer onlyGuicer;
}
