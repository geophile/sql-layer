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

import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.texpressions.TValidatedAggregator;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.google.common.base.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;

public final class TypesRegistryServiceImpl 
    implements TypesRegistryService, Service {

    private static TypesRegistryService INSTANCE = null;
    public static TypesRegistryService createRegistryService() {
        if (INSTANCE == null) {
            TypesRegistryServiceImpl registryService = new TypesRegistryServiceImpl();
            registryService.start();
            INSTANCE = registryService;
        }
        return INSTANCE;
    }
    public TypesRegistryServiceImpl() {
    }

    // TypesRegistryService interface

    @Override
    public TypesRegistry getTypesRegistry() {
        return typesRegistry;
    }

    @Override
    public OverloadResolver<TValidatedScalar> getScalarsResolver() {
        return scalarsResolver;
    }

    @Override
    public OverloadResolver<TValidatedAggregator> getAggregatesResolver() {
        return aggregatesResolver;
    }

    @Override
    public TCastResolver getCastsResolver() {
        return castsResolver;
    }

    @Override
    public TKeyComparable getKeyComparable(TClass left, TClass right) {
        if (left == null || right == null)
            return null;
        return keyComparableRegistry.getClass(left, right);
    }

    @Override
    public FunctionKind getFunctionKind(String name) {
        if (scalarsResolver.isDefined(name))
            return FunctionKind.SCALAR;
        else if (aggregatesResolver.isDefined(name))
            return FunctionKind.AGGREGATE;
        else
            return null;
    }

    // Service interface

    @Override
    public void start() {
        InstanceFinder registry;
        try {
            registry = new ReflectiveInstanceFinder();
        } catch (Exception e) {
            logger.error("while creating registry", e);
            throw new ServiceAlreadyStartedException("TypesRegistry");
        }
        start(registry);
    }

    @Override
    public void stop() {
        castsResolver = null;
        scalarsRegistry = null;
        aggreatorsRegistry = null;
        tClasses = null;
        keyComparableRegistry = null;
    }

    @Override
    public void crash() {
        stop();
    }

    // private methods

    void start(InstanceFinder finder) {
        tClasses = new HashSet<>(finder.find(TClass.class));

        typesRegistry = new TypesRegistry(tClasses);

        TCastsRegistry castsRegistry = new TCastsRegistry(tClasses, finder);
        castsResolver = new TCastResolver(castsRegistry);

        scalarsRegistry = ResolvablesRegistry.create(
                finder,
                castsResolver,
                TScalar.class,
                new Function<TScalar, TValidatedScalar>() {
                    @Override
                    public TValidatedScalar apply(TScalar input) {
                        return new TValidatedScalar(input);
                    }
                }
        );
        scalarsResolver = new OverloadResolver<>(scalarsRegistry, castsResolver);

        aggreatorsRegistry = ResolvablesRegistry.create(
                finder,
                castsResolver,
                TAggregator.class,
                new Function<TAggregator, TValidatedAggregator>() {
                    @Override
                    public TValidatedAggregator apply(TAggregator input) {
                        return new TValidatedAggregator(input);
                    }
                }
        );
        aggregatesResolver = new OverloadResolver<>(aggreatorsRegistry, castsResolver);

        keyComparableRegistry = new KeyComparableRegistry(finder);
    }

    // class state
    private static final Logger logger = LoggerFactory.getLogger(TypesRegistryServiceImpl.class);

    // object state

    private volatile TypesRegistry typesRegistry;
    private volatile TCastResolver castsResolver;
    private volatile ResolvablesRegistry<TValidatedAggregator> aggreatorsRegistry;
    private volatile OverloadResolver<TValidatedAggregator> aggregatesResolver;
    private volatile ResolvablesRegistry<TValidatedScalar> scalarsRegistry;
    private volatile OverloadResolver<TValidatedScalar> scalarsResolver;
    private volatile KeyComparableRegistry keyComparableRegistry;

    private volatile Collection<? extends TClass> tClasses;
}
