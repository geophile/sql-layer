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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.server.types.service.TypesRegistry;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TypeValidatorTest
{
    final TypesRegistry typesRegistry =
        TypesRegistryServiceImpl.createRegistryService().getTypesRegistry();

    @Test
    public void testTypeSupported() throws Exception {
        assertTrue(isTypeSupported("MCOMPAT", "int"));
        assertTrue(isTypeSupported("MCOMPAT", "varchar"));
        assertTrue(isTypeSupported("MCOMPAT", "text"));
        assertFalse(isTypeSupported("AKSQL", "result set"));
    }

    @Test
    public void testTypeSupportedAsIndex() throws Exception {
        assertTrue(isTypeSupportedAsIndex("MCOMPAT", "int"));
        assertTrue(isTypeSupportedAsIndex("MCOMPAT", "varchar"));
        assertFalse(isTypeSupportedAsIndex("MCOMPAT", "text"));
    }

    @Test
    public void testTypesCanBeJoined() throws Exception {
        // Every time can be joined to itself
        for(TClass t : typesRegistry.getTypeClasses()) {
            assertTrue(t.toString(), TypeValidator.isSupportedForJoin(t, t));
        }
        // All int types can be joined together except bigint unsigned
        final String intTypeNames[] = {"tinyint", "smallint", "int", "mediumint", "bigint"};
        for(String t1 : intTypeNames) {
            String t1U = t1 + " unsigned";
            for(String t2 : intTypeNames) {
                String t2U = t2 + " unsigned";
                boolean t1UIsBigint = "bigint unsigned".equals(t1U);
                boolean t2UIsBigint = "bigint unsigned".equals(t2U);
                assertTrue(t1+"->"+t2, canTypesBeJoined("MCOMPAT", t1, "MCOMPAT", t2));
                assertEquals(t1U + "->" + t2, !t1UIsBigint, canTypesBeJoined("MCOMPAT", t1U, "MCOMPAT", t2));
                assertEquals(t1 + "->" + t2U, !t2UIsBigint, canTypesBeJoined("MCOMPAT", t1, "MCOMPAT", t2U));
                assertEquals(t1U+"->"+t2U, (t1UIsBigint == t2UIsBigint), canTypesBeJoined("MCOMPAT", t1U, "MCOMPAT", t2U));
            }
        }
        // Check a few that cannot be
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "varchar"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "timestamp"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "decimal"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "double"));
        assertFalse(canTypesBeJoined("MCOMPAT", "char", "MCOMPAT", "binary"));
    }

    protected boolean isTypeSupported(String bundle, String name) {
        TClass tc = typesRegistry.getTypeClass(bundle, name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForColumn(tc);
    }

    protected boolean isTypeSupportedAsIndex(String bundle, String name) {
        TClass tc = typesRegistry.getTypeClass(bundle, name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForIndex(tc);
    }

    protected boolean canTypesBeJoined(String b1, String t1, String b2, String t2) {
        TClass c1 = typesRegistry.getTypeClass(b1, t1);
        assertNotNull(t1, c1);
        TClass c2 = typesRegistry.getTypeClass(b2, t2);
        assertNotNull(t2, c2);
        return TypeValidator.isSupportedForJoin(c1, c2);
    }
}
