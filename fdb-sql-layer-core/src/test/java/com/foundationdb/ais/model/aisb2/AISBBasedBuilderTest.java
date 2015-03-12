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
package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import static org.junit.Assert.*;

public class AISBBasedBuilderTest {
    protected TypesTranslator typesTranslator() {
        return MTypesTranslator.INSTANCE;
    }

    @Test
    public void example() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        AkibanInformationSchema ais =
            builder.defaultSchema("sch")
            .table("customer").colInt("cid").colString("name", 32).pk("cid")
            .table("order").colInt("oid").colInt("c2").colInt("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .table("item").colInt("iid").colInt("o_id").colInt("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
            .table("address").colInt("aid").colInt("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .ais();

        Group cGroup = ais.getGroup(new TableName("sch", "customer"));
        Table cTable = ais.getTable("sch", "customer");
        Table aTable = ais.getTable("sch", "address");
        Table oTable = ais.getTable("sch", "order");
        Table iTable = ais.getTable("sch", "item");

        assertNotNull("customer group", cGroup);
        assertEquals("customer group root", cGroup.getRoot(), cTable);

        assertEquals("address parent", cTable, aTable.getParentJoin().getParent());
        assertEquals("address join", "[JoinColumn(c_id -> cid)]", aTable.getParentJoin().getJoinColumns().toString());

        assertEquals("order parent", cTable, oTable.getParentJoin().getParent());
        assertEquals("order join", "[JoinColumn(c_id -> cid)]", oTable.getParentJoin().getJoinColumns().toString());

        assertEquals("item parent", oTable, iTable.getParentJoin().getParent());
        assertEquals("item join", "[JoinColumn(o_id -> oid), JoinColumn(o_c2 -> c2)]", iTable.getParentJoin().getJoinColumns().toString());
    }

    @Test
    public void exampleWithGroupIndexes() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        AkibanInformationSchema ais =
                builder.defaultSchema("sch")
                        .table("customer").colInt("cid").colString("name", 32).pk("cid")
                        .table("order").colInt("oid").colInt("c2").colInt("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                        .table("item").colInt("iid").colInt("o_id").colInt("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
                        .table("address").colInt("aid").colInt("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                .groupIndex("name_c2", Index.JoinType.LEFT).on("customer", "name").and("order", "c2")
                .groupIndex("iid_name_c2", Index.JoinType.LEFT).on("item", "iid").and("customer", "name").and("order", "c2")
                        .ais();

        Group cGroup = ais.getGroup(new TableName("sch", "customer"));
        Table cTable = ais.getTable("sch", "customer");
        Table aTable = ais.getTable("sch", "address");
        Table oTable = ais.getTable("sch", "order");
        Table iTable = ais.getTable("sch", "item");

        assertNotNull("customer group", cGroup);
        assertEquals("customer group root", cGroup.getRoot(), cTable);

        assertEquals("address parent", cTable, aTable.getParentJoin().getParent());
        assertEquals("address join", "[JoinColumn(c_id -> cid)]", aTable.getParentJoin().getJoinColumns().toString());

        assertEquals("order parent", cTable, oTable.getParentJoin().getParent());
        assertEquals("order join", "[JoinColumn(c_id -> cid)]", oTable.getParentJoin().getJoinColumns().toString());

        assertEquals("item parent", oTable, iTable.getParentJoin().getParent());
        assertEquals("item join", "[JoinColumn(o_id -> oid), JoinColumn(o_c2 -> c2)]", iTable.getParentJoin().getJoinColumns().toString());
    }
}
