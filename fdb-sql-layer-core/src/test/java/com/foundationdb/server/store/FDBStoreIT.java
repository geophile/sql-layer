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
package com.foundationdb.server.store;

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.FDBITBase;
import org.junit.Test;

import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class FDBStoreIT extends FDBITBase
{
    private static final String SCHEMA = "test";

    private void nextSequenceValue(final TableName seqName) {
        txnService().run(session(), new Runnable()  {
            @Override
            public void run() {
                Sequence s = ais().getSequence(seqName);
                fdbStore().nextSequenceValue(session(), s);
            }
        });
    }

    @Test
    public void dropSequenceMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();
        TableName seqName = new TableName(SCHEMA, "s");
        createSequence(SCHEMA, seqName.getTableName(), "");
        nextSequenceValue(seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        ddl().dropSequence(session(), seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropTableWithSerialMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();
        TableName tableName = new TableName(SCHEMA, "t");
        createTable(SCHEMA, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
        TableName serialSeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
        nextSequenceValue(serialSeqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        ddl().dropTable(session(), tableName);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropSchemaMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();

        TableName seqName = new TableName(SCHEMA, "s");
        createSequence(SCHEMA, seqName.getTableName(), "");
        nextSequenceValue(seqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

        TableName tableName = new TableName(SCHEMA, "t");
        createTable(SCHEMA, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
        TableName identitySeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
        nextSequenceValue(identitySeqName);
        assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

        ddl().dropSchema(session(), SCHEMA);
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }

    @Test
    public void dropNonSystemSchemasMaintainsSequenceCache() {
        int initial = fdbStore().getSequenceCacheMapSize();

        for(String schema : new String[] { "test1", "test2" }) {
            TableName seqName = new TableName(schema, "s");
            createSequence(schema, seqName.getTableName(), "");
            nextSequenceValue(seqName);
            assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));

            TableName tableName = new TableName(schema, "t");
            createTable(schema, tableName.getTableName(), "id SERIAL NOT NULL PRIMARY KEY");
            TableName identitySeqName = ais().getTable(tableName).getColumn("id").getIdentityGenerator().getSequenceName();
            nextSequenceValue(identitySeqName);
            assertThat(fdbStore().getSequenceCacheMapSize(), greaterThan(initial));
        }

        ddl().dropNonSystemSchemas(session());
        assertThat(fdbStore().getSequenceCacheMapSize(), lessThanOrEqualTo(initial));
    }
}