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

import com.foundationdb.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.foundationdb.util.JUnitUtils;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class GuiceInjectionTester {

    public <I> GuiceInjectionTester bind(Class<I> anInterface, Class<? extends I> anImplementation) {
        configHandler.bind(anInterface.getName(), anImplementation.getName(), null);
        return this;
    }

    public <I extends ServiceManagerBase> GuiceInjectionTester manager(Class<I> serviceManagerInterfaceClass,
                                        I serviceManager) {
        this.serviceManagerInterfaceClass = serviceManagerInterfaceClass;
        this.serviceManager = serviceManager;
        return this;
    }

    public <I> GuiceInjectionTester prioritize(Class<I> anInterface) {
        configHandler.prioritize(anInterface.getName());
        return this;
    }

    @SuppressWarnings("unchecked")
    public GuiceInjectionTester startAndStop(Class<?>... requiredClasses) {
        for (Class<?> requiredClass : requiredClasses) {
            configHandler.require(requiredClass.getName());
        }
        try {
            guicer = Guicer.forServices((Class<ServiceManagerBase>)serviceManagerInterfaceClass, serviceManager,
                                        configHandler.serviceBindings(true), configHandler.priorities(),
                                        Collections.<Module>emptyList());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (Class<?> requiredClass : guicer.directlyRequiredClasses()) {
            guicer.get(requiredClass, shutdownHook);
        }
        guicer.stopAllServices(shutdownHook);
        return this;
    }

    public GuiceInjectionTester checkDependencies(Class<?> aClass, Class<?>... itsDependencies) {
        for (Class<?> dependency : itsDependencies) {
            checkSingleDependency(aClass, dependency);
        }

        // alternate method
        List<Class<?>> allClassesExpected = new ArrayList<>();
        allClassesExpected.add(aClass);
        Collections.addAll(allClassesExpected, itsDependencies);
        List<Class<?>> allClassesActual = new ArrayList<>();
        for (Object instance : guicer.dependenciesFor(aClass)) {
            allClassesActual.add(instance.getClass());
        }
        JUnitUtils.equalCollections("for " + aClass, allClassesExpected, allClassesActual);
        return this;
    }

    public GuiceInjectionTester check(Class<?>... expectedClasses) {
        List<Class<?>> expectedList = Arrays.asList(expectedClasses);
        checkExactContents("shutdown", expectedList, shutdownHook.stoppedClasses());
        return this;
    }

    private void checkExactContents(String whichList, List<Class<?>> expectedList, List<Class<?>> actualList) {
        if (expectedList.size() != actualList.size()) {
            JUnitUtils.equalCollections(whichList + " lists not of same size", expectedList, actualList);
        }
        assertEquals(whichList + " size", expectedList.size(), actualList.size());
        assertTrue(whichList + ": " + l(expectedList) + " != " + l(actualList), actualList.containsAll(expectedList));
    }

    private void checkSingleDependency(Class<?> aClass, Class<?> itsDependency) {
    // The class should appear after the dependency in startup, and before it for shutdown
        List<Class<?>> shutdownOrder = shutdownHook.stoppedClasses();
        int aClassShutdownOrder = findInList(aClass, shutdownOrder);
        int dependencyShutdownOrder = findInList(itsDependency, shutdownOrder);
        assertTrue(
                String.format("%s stopped before %s: %s", n(itsDependency), n(aClass), l(shutdownOrder)),
                aClassShutdownOrder < dependencyShutdownOrder
        );
    }

    private static String n(Class<?> aClass) {
        return aClass.getSimpleName();
    }

    private List<String> l(List<Class<?>> list) {
        List<String> simpleNames = new ArrayList<>();
        for (Class<?> aClass : list) {
            simpleNames.add(n(aClass));
        }
        return simpleNames;
    }

    private int findInList(Class<?> aClass, List<Class<?>> list) {
        int index = list.indexOf(aClass);
        assertTrue(aClass + " not in list " + list, index >= 0);
        return index;
    }

    private final DefaultServiceConfigurationHandler configHandler = new DefaultServiceConfigurationHandler();
    private final ListOnShutdown shutdownHook = new ListOnShutdown();
    private Class<? extends ServiceManagerBase> serviceManagerInterfaceClass;
    private ServiceManagerBase serviceManager;
    private Guicer guicer;

    // nested classes

    private static class ListOnShutdown implements Guicer.ServiceLifecycleActions<Object> {

        public List<Class<?>> stoppedClasses() {
            return stoppedClasses;
        }

        @Override
        public void onStart(Object service) {
        }

        @Override
        public void onShutdown(Object service) {
            stoppedClasses.add(service.getClass());
        }

        @Override
        public Object castIfActionable(Object object) {
            return object;
        }

        private final List<Class<?>> stoppedClasses = new ArrayList<>();
    }

}
