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
package com.foundationdb.server.store.format.protobuf;


import java.math.BigDecimal;
import java.util.UUID;

import org.junit.Test;

import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.SchemaFactory;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import static org.junit.Assert.assertTrue;

public class ProtobufRowConverterTest {

    @Test
    public void testSimple() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(128), d DOUBLE)");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        Table t = ais.getTable(new TableName(SCHEMA, "t"));
        RowType tRowType = schema.tableRowType(t);
        ProtobufRowConverter converter = converter(g);
        encodeDecode(ais, converter, tRowType,
                     1L, "Fred", 3.14);
    }

    @Test
    public void testGroup() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE c(cid INT PRIMARY KEY NOT NULL, name VARCHAR(128));" +
          "CREATE TABLE o(oid INT PRIMARY KEY NOT NULL, cid INT, GROUPING FOREIGN KEY(cid) REFERENCES c(cid));" +
          "CREATE TABLE i(iid INT PRIMARY KEY NOT NULL, oid INT, GROUPING FOREIGN KEY(oid) REFERENCES o(oid), sku VARCHAR(16));");
        Group coi = ais.getGroup(new TableName(SCHEMA, "c"));
        RowType cRowType = schema.tableRowType(ais.getTable(new TableName (SCHEMA, "c")));
        RowType oRowType = schema.tableRowType(ais.getTable(new TableName (SCHEMA, "o")));
        RowType iRowType = schema.tableRowType(ais.getTable(new TableName (SCHEMA, "i")));
        
        ProtobufRowConverter converter = converter(coi);
        encodeDecode(ais, converter, cRowType,
                     1L, "Fred");
        encodeDecode(ais, converter, oRowType,
                     101L, 1L);
        encodeDecode(ais, converter, iRowType,
                     10101L, 101L, "P100");
    }

    @Test
    public void testDecimal() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, n1 DECIMAL(6,2), n2 DECIMAL(20,10))");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        RowType tRowType = schema.tableRowType(g.getRoot()); 
        ProtobufRowConverter converter = converter(g);
        encodeDecode(ais, converter, tRowType,
                     1L, new BigDecimal("3.14"), new BigDecimal("1234567890.0987654321"));
    }

    @Test
    public void testBlob() throws Exception {
        AkibanInformationSchema ais = ais(
                "CREATE TABLE b(id INT PRIMARY KEY NOT NULL, bl blob)");
        Group g = ais.getGroup(new TableName(SCHEMA, "b"));
        RowType tRowType = schema.tableRowType(g.getRoot());
        ProtobufRowConverter converter = converter(g);
        byte[] ba = new byte[100];
        ba[0] = BlobRef.SHORT_LOB;
        encodeDecode(ais, converter, tRowType,
                1, ba);        
    }
    
    @Test
    public void testNulls() throws Exception {
        AkibanInformationSchema ais = ais(
          "CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(128) DEFAULT 'abc')");
        Group g = ais.getGroup(new TableName(SCHEMA, "t"));
        RowType tRowType = schema.tableRowType(g.getRoot());
        ProtobufRowConverter converter = converter(g);
        encodeDecode(ais, converter, tRowType,
                     1L, "Barney");
        encodeDecode(ais, converter, tRowType,
                     2L, null);
    }
    
    
    protected void encodeDecode(AkibanInformationSchema ais,
            ProtobufRowConverter converter, 
            RowType rowType, 
            Object... values) 
                    throws Exception {
        Row rowIn = new ValuesHolderRow(rowType, values);
        DynamicMessage msg = converter.encode(rowIn);
        Row rowOut = converter.decode(msg);
        assertTrue (rowIn.compareTo(rowOut, 0, 0, rowType.nFields()) == 0);
        
    }
    protected ProtobufRowConverter converter(Group g) throws Exception {
        AISToProtobuf a2p = new AISToProtobuf(ProtobufRowFormat.Type.GROUP_MESSAGE);
        a2p.addGroup(g);
        FileDescriptorSet set = a2p.build();
        FileDescriptor gdesc = FileDescriptor.buildFrom(set.getFile(0),
                                                        ProtobufStorageDescriptionHelper.DEPENDENCIES);
        return ProtobufRowConverter.forGroup(g, gdesc);
    }
    private static final String SCHEMA = "test";

    protected AkibanInformationSchema ais(String ddl) {
        AkibanInformationSchema ais = new SchemaFactory(SCHEMA).aisWithTableStatus(ddl);
        for (Table table : ais.getTables().values()) {
            if (!table.hasVersion()) {
                table.setVersion(0);
            }
            if (table.getUuid() == null)
                table.setUuid(UUID.randomUUID());
            for (Column column : table.getColumnsIncludingInternal()) {
                if (column.getUuid() == null)
                    column.setUuid(UUID.randomUUID());
            }
        }
        schema = new Schema (ais);
        return ais;
    }
    private Schema schema;
}
