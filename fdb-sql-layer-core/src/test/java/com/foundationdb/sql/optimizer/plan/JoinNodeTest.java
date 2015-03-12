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
package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JoinNodeTest {

    protected TableSource source1;
    protected TableSource source2;

    @Before
    public void setup() {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        AkibanInformationSchema ais = AISBBasedBuilder.create("s", typesTranslator)
                                                      .table("t1")
                                                      .colString("first_name", 32)
                                                      .colString("last_name", 32)
                                                      .table("t2")
                                                      .colString("first_name", 32)
                                                      .colString("last_name", 32)
                                                      .ais();
        Table table1 = ais.getTable("s", "t1");
        TableNode node1 = new TableNode(table1, new TableTree());
        source1 = new TableSource(node1, true, "t1");

        Table table2 = ais.getTable("s", "t2");
        TableNode node2 = new TableNode(table2, new TableTree());
        source2 = new TableSource(node2, true, "t2");
    }

    @Test
    public void TestDuplicate() {
        JoinNode joinNode = new JoinNode((Joinable)source1, (Joinable)source2, JoinNode.JoinType.LEFT);
        JoinNode duplicate = (JoinNode)joinNode.duplicate();

        assertEquals(joinNode.getJoinType(), duplicate.getJoinType());
        assertEquals(joinNode.getJoinConditions(), duplicate.getJoinConditions());
        assertEquals(joinNode.getImplementation(), duplicate.getImplementation());
    }
}
