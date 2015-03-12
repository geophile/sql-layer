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
package com.foundationdb.sql.aisddl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.View;
import com.foundationdb.server.error.UndefinedViewException;
import com.foundationdb.server.error.ViewReferencesExist;

import java.util.Collection;

public class ViewDDLIT extends AISDDLITBase {

    @Before
    public void createTable() throws Exception {
        executeDDL("CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(10))");
    }

    @Test
    public void testCreate() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertNotNull(v);
        assertEquals(2, v.getColumns().size());
        assertEquals("id", v.getColumn(0).getName());
        assertEquals("s", v.getColumn(1).getName());
        Table t = ais().getTable(SCHEMA_NAME, "t");
        assertEquals(1, v.getTableReferences().size());
        Collection<Column> tcols = v.getTableColumnReferences(t);
        assertNotNull(tcols);
        assertEquals(2, tcols.size());
    }

    @Test
    public void testCreateWithFunctions() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT current_session_id() AS r");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertNotNull(v);
        assertEquals(1, v.getColumns().size());
        assertEquals("r", v.getColumn(0).getName());
    }

    @Test(expected=UndefinedViewException.class)
    public void testDropNonexistent() throws Exception {
        executeDDL("DROP VIEW no_such_view");
    }

    @Test
    public void testDropOptionalNonexistent() throws Exception {
        executeDDL("DROP VIEW IF EXISTS no_such_view");
    }

    @Test
    public void testDropExists() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ais().getView(SCHEMA_NAME, "v"));

        executeDDL("DROP VIEW v");
        assertNull(ais().getView(SCHEMA_NAME, "v"));
    }

    @Test
    public void testDropOptionalExists() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ais().getView(SCHEMA_NAME, "v"));

        executeDDL("DROP VIEW IF EXISTS v");
        assertNull(ais().getView(SCHEMA_NAME, "v"));
    }

    @Test(expected=ViewReferencesExist.class)
    public void testDropTableReferenced() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        executeDDL("DROP TABLE t");
    }

    @Test(expected=ViewReferencesExist.class)
    public void testDropViewReferenced() throws Exception {
        executeDDL("CREATE VIEW v1 AS SELECT * FROM t");
        executeDDL("CREATE VIEW v2 AS SELECT * FROM v1");
        executeDDL("DROP VIEW v1");
    }

    @Test
    public void testViewColumnNames() throws Exception {
        executeDDL("CREATE VIEW v(x,y) AS SELECT id, s FROM t");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertEquals("x", v.getColumns().get(0).getName());
        assertEquals("y", v.getColumns().get(1).getName());
    }
}
