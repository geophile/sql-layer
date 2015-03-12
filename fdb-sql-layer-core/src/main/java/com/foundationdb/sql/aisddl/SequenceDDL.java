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

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.parser.CreateSequenceNode;
import com.foundationdb.sql.parser.DropSequenceNode;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.sql.types.TypeId;

import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

public class SequenceDDL {
    private SequenceDDL() { }
    
    public static void createSequence (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    CreateSequenceNode node) {
        final TableName seqName = DDLHelper.convertName(defaultSchemaName, node.getObjectName());
        // Implementation defined if unspecified
        long minValue = (node.getMinValue() != null) ? node.getMinValue() : 1;
        long maxValue = (node.getMaxValue() != null) ? node.getMaxValue() : Long.MAX_VALUE;
        // Standard compliant defaults
        long startWith = (node.getStartWith() != null) ? node.getStartWith() : minValue;
        long incBy = (node.getIncrementBy() != null) ? node.getIncrementBy() : 1;
        boolean isCycle = (node.isCycle() != null) ? node.isCycle() : false;
        // Sequence doesn't have a backing SQL data type so just limit the max if one was given
        if((node.getMaxValue() == null) && (node.getDataType() != null)) {
            TypeId typeId = node.getDataType().getTypeId();
            if(typeId == TypeId.TINYINT_ID) {
                maxValue = Byte.MAX_VALUE;
            } else if(typeId == TypeId.SMALLINT_ID) {
                maxValue = Short.MAX_VALUE;
            } else if(typeId == TypeId.INTEGER_ID) {
                maxValue = Integer.MAX_VALUE;
            }
            // else keep long max
        }
        AISBuilder builder = new AISBuilder();
        builder.sequence(seqName.getSchemaName(), seqName.getTableName(), startWith, incBy, minValue, maxValue, isCycle);
        Sequence sequence = builder.akibanInformationSchema().getSequence(seqName);
        if (node.getStorageFormat() != null) {
            TableDDL.setStorage(ddlFunctions, sequence, node.getStorageFormat());
        }
        ddlFunctions.createSequence(session, sequence);
    }
    
    public static void dropSequence (DDLFunctions ddlFunctions,
                                        Session session,
                                        String defaultSchemaName,
                                        DropSequenceNode dropSequence,
                                        QueryContext context) {
        final TableName sequenceName = DDLHelper.convertName(defaultSchemaName, dropSequence.getObjectName());

        Sequence sequence = ddlFunctions.getAIS(session).getSequence(sequenceName);
        if((sequence == null) &&
           skipOrThrow(context, dropSequence.getExistenceCheck(), sequence, new NoSuchSequenceException(sequenceName))) {
            return;
        }

        ddlFunctions.dropSequence(session, sequenceName);
    }
}
