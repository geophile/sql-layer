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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.TypesTestClass;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class TypesRegistryServiceImplTest {
    private final TClass CLASS_A = new TypesTestClass("A");
    private final TClass CLASS_B = new TypesTestClass("B");
    private final TClass CLASS_C = new TypesTestClass("C");
    private final TClass CLASS_D = new TypesTestClass("D");
    private final TClass CLASS_E = new TypesTestClass("E");

    @Test
    public void typesRegistry() {
        InstanceFinder finder = createFinder();
        TypesRegistry typesRegistry = new TypesRegistry(finder.find(TClass.class));
        assertEquals("A", CLASS_A, typesRegistry.getTypeClass("testbundle", "a"));
        assertEquals("B", CLASS_B, typesRegistry.getTypeClass("testbundle", "B"));
        assertEquals("C", CLASS_C, typesRegistry.getTypeClass("testbundle", "c"));
        assertEquals("D", CLASS_D, typesRegistry.getTypeClass("testbundle", "D"));
        assertEquals("E", CLASS_E, typesRegistry.getTypeClass("testbundle", "e"));
        assertNull("Z", typesRegistry.getTypeClass("testbundle", "Z"));
    }

    @Test
    public void createSelfCasts() {
        InstanceFinder finder = createFinder();
        Map<TClass, Map<TClass, TCast>> casts = TCastsRegistry.createCasts(finder.find(TClass.class), finder);
        checkAll(casts,
                new CastCheck(CLASS_A, CLASS_A),
                new CastCheck(CLASS_B, CLASS_B),
                new CastCheck(CLASS_C, CLASS_C),
                new CastCheck(CLASS_D, CLASS_D),
                new CastCheck(CLASS_E, CLASS_E));
    }

    private InstanceFinderBuilder createFinder() {
        InstanceFinderBuilder finder = new InstanceFinderBuilder();
        finder.put(TClass.class, MString.VARCHAR);
        finder.put(TClass.class, CLASS_A);
        finder.put(TClass.class, CLASS_B);
        finder.put(TClass.class, CLASS_C);
        finder.put(TClass.class, CLASS_D);
        finder.put(TClass.class, CLASS_E);
        return finder;
    }

    @Test
    public void simpleCastPath() {
        InstanceFinderBuilder finder = createFinder();
        TCastPath path = TCastPath.create(CLASS_A, CLASS_B, CLASS_C);
        finder.put(TCastPath.class, path);
        Map<TClass, Map<TClass, TCast>> casts = TCastsRegistry.createCasts(finder.find(TClass.class), finder);
        singleStepCasts(casts);
        TCastsRegistry.createDerivedCasts(casts, finder);
        checkAll(casts,
                // self casts
                new CastCheck(CLASS_A, CLASS_A),
                new CastCheck(CLASS_B, CLASS_B),
                new CastCheck(CLASS_C, CLASS_C),
                new CastCheck(CLASS_D, CLASS_D),
                new CastCheck(CLASS_E, CLASS_E),
                // single-step casts
                new CastCheck(CLASS_A, CLASS_B),
                new CastCheck(CLASS_B, CLASS_C),
                new CastCheck(CLASS_C, CLASS_D),
                new CastCheck(CLASS_D, CLASS_E),
                // cast path
                new CastCheck(CLASS_A, CLASS_C)
        );
    }

    @Test
    public void jumpingCastPath() {
        InstanceFinderBuilder finder = createFinder();
        finder.put(TCastPath.class, TCastPath.create(CLASS_A, CLASS_B, CLASS_C, CLASS_D, CLASS_E));

        Map<TClass, Map<TClass, TCast>> casts = TCastsRegistry.createCasts(finder.find(TClass.class), finder);
        singleStepCasts(casts);
        putCast(new BogusCast(CLASS_A, CLASS_D), casts);
        TCastsRegistry.createDerivedCasts(casts, finder);
        checkAll(casts,
                // self casts
                new CastCheck(CLASS_A, CLASS_A),
                new CastCheck(CLASS_B, CLASS_B),
                new CastCheck(CLASS_C, CLASS_C),
                new CastCheck(CLASS_D, CLASS_D),
                new CastCheck(CLASS_E, CLASS_E),
                // cast path from A
                new CastCheck(CLASS_A, CLASS_B),
                new CastCheck(CLASS_A, CLASS_C),
                new CastCheck(CLASS_A, CLASS_D),
                new CastCheck(CLASS_A, CLASS_E),
                // cast path from B
                new CastCheck(CLASS_B, CLASS_C),
                new CastCheck(CLASS_B, CLASS_D),
                new CastCheck(CLASS_B, CLASS_E),
                // cast path from C
                new CastCheck(CLASS_C, CLASS_D),
                new CastCheck(CLASS_C, CLASS_E),
                // cast path from D
                new CastCheck(CLASS_D, CLASS_E)
        );
    }

    private void singleStepCasts(Map<TClass, Map<TClass, TCast>> casts) {
        putCast(new BogusCast(CLASS_A, CLASS_B), casts);
        putCast(new BogusCast(CLASS_B, CLASS_C), casts);
        putCast(new BogusCast(CLASS_C, CLASS_D), casts);
        putCast(new BogusCast(CLASS_D, CLASS_E), casts);
    }

    private void putCast(TCast cast, Map<TClass, Map<TClass, TCast>> outMap) {
        Object o = outMap.get(cast.sourceClass()).put(cast.targetClass(), cast);
        assert o == null : "putting " + cast + " into " + outMap; // shouldn't happen
    }

    private void checkAll(Map<TClass, Map<TClass, TCast>> actual, CastCheck... expected) {
        // translate the SelfCastCheck[] to a Map

        // create the full expected list, which includes VARCHAR -> VARCHAR and VARCHAR <-> T for each type
        // in the actuals keys
        List<CastCheck> allExpected = new ArrayList<>();
        Collections.addAll(allExpected, expected);
        for (TClass src : actual.keySet()) {
            if (src != MString.VARCHAR) {
                allExpected.add(new CastCheck(src, MString.VARCHAR));
                allExpected.add(new CastCheck(MString.VARCHAR, src));
            }
        }
        allExpected.add(new CastCheck(MString.VARCHAR, MString.VARCHAR));

        Map<TClass,Map<TClass,CastCheck>> expectedMap = tClassMap();
        for (CastCheck check : allExpected) {
            Map<TClass, CastCheck> map = expectedMap.get(check.source);
            if (map == null) {
                map = tClassMap();
                expectedMap.put(check.source, map);
            }
            CastCheck old = map.put(check.target, check);
            assertNull("duplicate for " + check, old);
        }

        // Translate the casts map to use SelfCastCheck
        Map<TClass,Map<TClass,CastCheck>> actualsMap = tClassMap();
        for (Map.Entry<TClass,Map<TClass,TCast>> bySourceEntry : actual.entrySet()) {
            TClass source = bySourceEntry.getKey();
            Map<TClass, CastCheck> translated = tClassMap();
            Object old = actualsMap.put(source, translated);
            assert old == null : actual; // shouldn't happen!
            for (Map.Entry<TClass,TCast> byTargetEntry : bySourceEntry.getValue().entrySet()) {
                TClass target = byTargetEntry.getKey();
                TCast cast = byTargetEntry.getValue();
                CastCheck check = new CastCheck(cast.sourceClass(), cast.targetClass());
                old = translated.put(target, check);
                assertNull("duplicate with " + bySourceEntry, old);
            }
        }

        // now just check the two maps
        assertEquals("casts map", toString(expectedMap), toString(actualsMap)); // easy-to-view diff in IDEs
        assertEquals("casts map", expectedMap, actualsMap);
    }

    private String toString(Map<TClass, Map<TClass, CastCheck>> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<TClass, Map<TClass, CastCheck>> entry : map.entrySet()) {
            sb.append(entry.getKey()).append('\n');
            for (Map.Entry<TClass,CastCheck> subentry: entry.getValue().entrySet()) {
                sb.append("    ").append(subentry).append('\n');
            }
        }
        return sb.toString();
    }

    private <V> TreeMap<TClass, V> tClassMap() {
        return new TreeMap<>(tClassComparator);
    }

    private static class CastCheck {

        @Override
        public String toString() {
            return "cast(" + source + " to " + target + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CastCheck that = (CastCheck) o;

            return source.equals(that.source) && target.equals(that.target);

        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + target.hashCode();
            return result;
        }

        private CastCheck(TClass source, TClass target) {
            this.source = source;
            this.target = target;
        }

        private final TClass source;
        private final TClass target;
    }

    private static class BogusCast extends TCastBase {
        private BogusCast(TClass sourceClass, TClass targetClass) {
            super(sourceClass, targetClass, Constantness.UNKNOWN);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            throw new UnsupportedOperationException();
        }
    }

    private Comparator<TClass> tClassComparator = new Comparator<TClass>() {
        @Override
        public int compare(TClass o1, TClass o2) {
            return o1.toString().compareTo(o2.toString());
        }
    };
}
