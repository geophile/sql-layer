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
package com.foundationdb.server.test.it.bugs.bug720768;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GroupNameCollisionIT extends ITBase {
    @Test
    public void tablesWithSameNames() {

        try {
            createTable("s1", "t", "id int not null primary key");
            createTable("s2", "t", "id int not null primary key");
            createTable("s1", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
            createTable("s2", "c", "id int not null primary key, pid int",
                    "GROUPING FOREIGN KEY (pid) REFERENCES t(id)");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        AkibanInformationSchema ais = ddl().getAIS(session());
        final Group group1 = ais.getTable("s1", "t").getGroup();
        final Group group2 = ais.getTable("s2", "t").getGroup();
        if (group1.getName().equals(group2.getName())) {
            fail("same group names: " + group1 + " and " + group2);
        }

        Table s1T = ais.getTable("s1", "t");
        Table s1C = ais.getTable("s1", "c");
        Table s2T = ais.getTable("s2", "t");
        Table s2C = ais.getTable("s2", "c");

        assertEquals("s1.t root", s1T, group1.getRoot());
        assertEquals("s1.c parent", s1T, s1C.getParentJoin().getParent());
        assertEquals("s1.c join cols", "[JoinColumn(pid -> id)]", s1C.getParentJoin().getJoinColumns().toString());

        assertEquals("s2.t root", s2T, group2.getRoot());
        assertEquals("s2.c parent", s2T, s2C.getParentJoin().getParent());
        assertEquals("s2.c join cols", "[JoinColumn(pid -> id)]", s2C.getParentJoin().getJoinColumns().toString());
    }
}
