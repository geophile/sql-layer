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

import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.*;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.AISBinderContext;
import com.foundationdb.sql.optimizer.AISViewDefinition;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.DropViewNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.qp.operator.QueryContext;

import java.util.Collection;
import java.util.Map;

import static com.foundationdb.sql.aisddl.DDLHelper.convertName;
import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

/** DDL operations on Views */
public class ViewDDL
{
    private ViewDDL() {
    }

    public static void createView(DDLFunctions ddlFunctions,
                                  Session session,
                                  String defaultSchemaName,
                                  CreateViewNode createView,
                                  AISBinderContext binderContext,
                                  QueryContext context, ServerSession server) {
        TableName fullName = convertName(defaultSchemaName, createView.getObjectName());
        String schemaName = fullName.getSchemaName();
        String viewName = fullName.getTableName();

        View curView = ddlFunctions.getAIS(session).getView(schemaName, viewName);
        if((curView != null) &&
           skipOrThrow(context, createView.getExistenceCheck(), curView, new DuplicateViewException(schemaName, viewName))) {
            return;
        }

        TypesTranslator typesTranslator = ddlFunctions.getTypesTranslator();
        AISViewDefinition viewdef = binderContext.getViewDefinition(createView, server);
        Map<TableName,Collection<String>> tableColumnReferences = viewdef.getTableColumnReferences();
        AISBuilder builder = new AISBuilder();
        builder.view(schemaName, viewName, viewdef.getQueryExpression(), 
                     binderContext.getParserProperties(schemaName), tableColumnReferences);
        int colpos = 0;
        for (ResultColumn rc : viewdef.getResultColumns()) {
            DataTypeDescriptor type = rc.getType();
            if (type == null) {
                if (rc.getExpression().getNodeType() != NodeTypes.UNTYPED_NULL_CONSTANT_NODE)
                    throw new AkibanInternalException(rc.getName() + " has unknown type");
                type = new DataTypeDescriptor(TypeId.CHAR_ID, true, 0);
            }
            TableDDL.addColumn(builder, typesTranslator,
                               schemaName, viewName, rc.getName(), colpos++,
                               type, null, null);
        }
        View view = builder.akibanInformationSchema().getView(schemaName, viewName);
        ddlFunctions.createView(session, view);
    }

    public static void dropView (DDLFunctions ddlFunctions,
                                 Session session, 
                                 String defaultSchemaName,
                                 DropViewNode dropView,
                                 AISBinderContext binderContext,
                                 QueryContext context) {
        TableName viewName = convertName(defaultSchemaName, dropView.getObjectName());
        View curView = ddlFunctions.getAIS(session).getView(viewName);

        if((curView == null) &&
           skipOrThrow(context, dropView.getExistenceCheck(), curView, new UndefinedViewException(viewName))) {
            return;
        }

        checkDropTable(ddlFunctions, session, viewName);
        ddlFunctions.dropView(session, viewName);
    }

    public static void checkDropTable(DDLFunctions ddlFunctions, Session session, 
                                      TableName name) {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        Columnar table = ais.getColumnar(name);
        if (table == null) return;
        for (View view : ais.getViews().values()) {
            if (view.referencesTable(table)) {
                throw new ViewReferencesExist(view.getName().getSchemaName(),
                                              view.getName().getTableName(),
                                              table.getName().getSchemaName(),
                                              table.getName().getTableName());
            }
        }
    }

}
