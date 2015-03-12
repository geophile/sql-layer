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

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.ais.model.Schema;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.ReferencedSchemaException;
import com.foundationdb.server.error.DuplicateSchemaException;
import com.foundationdb.server.error.NoSuchSchemaException;
import com.foundationdb.server.service.session.Session;

import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.DropSchemaNode;
import com.foundationdb.sql.parser.StatementType;
import com.foundationdb.qp.operator.QueryContext;

import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

public class SchemaDDL {
    private SchemaDDL () {
    }
    
    public static void createSchema (AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateSchemaNode createSchema,
                                   QueryContext context)
    {
        final String schemaName = createSchema.getSchemaName();

        Schema curSchema = ais.getSchema(schemaName);
        if((curSchema != null) &&
           skipOrThrow(context, createSchema.getExistenceCheck(), curSchema, new DuplicateSchemaException(schemaName))) {
            return;
        }

        // If you get to this point, the schema name isn't being used by any user or group table
        // therefore is a valid "new" schema. 
        // TODO: update the AIS to store the new schema. 
    }
    
    public static void dropSchema (DDLFunctions ddlFunctions,
            Session session,
            DropSchemaNode dropSchema,
            QueryContext context)
    {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        final String schemaName = dropSchema.getSchemaName();

        Schema curSchema = ais.getSchema(schemaName);
        if((curSchema == null) &&
           skipOrThrow(context, dropSchema.getExistenceCheck(), curSchema, new NoSuchSchemaException(schemaName))) {
            return;
        }
        // 1 == RESTRICT, meaning no drop if the schema isn't empty
        if (dropSchema.getDropBehavior() == StatementType.DROP_RESTRICT ||
                dropSchema.getDropBehavior() == StatementType.DROP_DEFAULT)
            throw new ReferencedSchemaException(schemaName);
        // If the schema isn't used by any existing tables, it has effectively
        // been dropped, so the drop "succeeds".
        else if (dropSchema.getDropBehavior() == StatementType.DROP_CASCADE)
            ddlFunctions.dropSchema(session, schemaName);
    }
}
