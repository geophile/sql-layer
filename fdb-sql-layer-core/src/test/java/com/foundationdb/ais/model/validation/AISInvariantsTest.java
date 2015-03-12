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
package com.foundationdb.ais.model.validation;

import com.foundationdb.server.error.DuplicateIndexColumnException;
import org.junit.Test;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TestTypesRegistry;

public class AISInvariantsTest {
    private AISBuilder builder;
    
    private TInstance intType = TestTypesRegistry.MCOMPAT
        .getTypeClass("MCOMPAT", "INT").instance(false);

    @Test (expected=InvalidOperationException.class)
    public void testDuplicateTables() {
        builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        
        builder.table("test", "t1");
    }

    @Test (expected=InvalidOperationException.class)
    public void testDuplicateColumns() {
        builder = new AISBuilder();
        
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c1", 1, intType, false, null, null);

    }
    
    //@Test (expected=InvalidOperationException.class)
    public void testDuplicateColumnPos() {
        builder = new AISBuilder();
        
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c2", 0, intType, false, null, null);
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateIndexes() {
        builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, true, null, null);
        builder.column("test", "t1", "c2", 1, intType, false, null, null);
        
        builder.pk("test", "t1");
        builder.indexColumn("test", "t1", Index.PRIMARY, "c1", 0, true, null);
        builder.pk("test", "t1");
    }
    
    @Test (expected=InvalidOperationException.class)
    public void testDuplicateGroup() {
        builder = new AISBuilder();
        builder.createGroup("test", "test");
        builder.createGroup("test", "test");
    }

    @Test (expected=DuplicateIndexColumnException.class)
    public void testDuplicateColumnsTableIndex() {
        builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, false, null, null);
        builder.index("test", "t1", "c1_index");
        builder.indexColumn("test", "t1", "c1_index", "c1", 0, true, null);
        builder.indexColumn("test", "t1", "c1_index", "c1", 1, true, null);
    }

    @Test (expected=DuplicateIndexColumnException.class)
    public void testDuplicateColumnsGroupIndex() {
        builder = createSimpleValidGroup();
        builder.groupIndex("t1", "y_x", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("t1", "y_x", "test", "t2", "y", 0);
        builder.groupIndexColumn("t1", "y_x", "test", "t2", "y", 1);
    }

    @Test
    public void testDuplicateColumnNamesButValidGroupIndex() {
        builder = createSimpleValidGroup();
        builder.groupIndex("t1", "y_y", false, Index.JoinType.LEFT);
        builder.groupIndexColumn("t1", "y_y", "test", "t2", "y", 0);
        builder.groupIndexColumn("t1", "y_y", "test", "t1", "y", 1);
    }

    private AISBuilder createSimpleValidGroup() {
        AISBuilder builder = new AISBuilder();
        builder.table("test", "t1");
        builder.column("test", "t1", "c1", 0, intType, false, null, null);
        builder.column("test", "t1", "x", 1, intType, false, null, null);
        builder.column("test", "t1", "y", 2, intType, false, null, null);
        builder.pk("test", "t1");
        builder.indexColumn("test", "t1", Index.PRIMARY, "c1", 0, true, null);
        builder.table("test", "t2");
        builder.column("test", "t2", "c1", 0, intType, false, null, null);
        builder.column("test", "t2", "c2", 1, intType, false, null, null);
        builder.column("test", "t2", "y", 2, intType, false, null, null);
        builder.basicSchemaIsComplete();

        builder.createGroup("t1", "test");
        builder.addTableToGroup("t1", "test", "t1");
        builder.joinTables("t2/t1", "test", "t1", "test", "t2");
        builder.joinColumns("t2/t1", "test", "t2", "c1", "test", "t2", "c2");
        builder.addJoinToGroup("t1", "t2/t1", 0);
        builder.groupingIsComplete();
        return builder;
    }
}
