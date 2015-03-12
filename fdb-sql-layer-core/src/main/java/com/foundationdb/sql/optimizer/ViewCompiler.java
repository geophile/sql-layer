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
package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.ResultSet;
import com.foundationdb.sql.optimizer.plan.SelectQuery;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.TypeResolver;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromSubquery;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.CursorNode.UpdateMode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.server.ServerOperatorCompiler;
import com.foundationdb.sql.server.ServerSession;

public class ViewCompiler extends ServerOperatorCompiler {

    public ViewCompiler(ServerSession server, KeyCreator keyCreator) {
        initServer(server, keyCreator);
    }

    protected void initAIS(AkibanInformationSchema ais, AISBinderContext context, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setContext(context);
    }

    /** Run just enough rules to get to TypeResolver, then set types. */
    protected void findAndSetTypes(AISViewDefinition view) {
        FromSubquery fromSubquery = view.getSubquery();

        // put the SELECT in a cursorNode to enable bindAndTransform/statementLoader/etc on it.
        CursorNode cursorNode = new CursorNode();
        cursorNode.init("SELECT",
                fromSubquery.getSubquery(),
                view.getName().getFullTableName(),
                fromSubquery.getOrderByList(),
                fromSubquery.getOffset(),
                fromSubquery.getFetchFirst(),
                UpdateMode.UNSPECIFIED,
                null);
        cursorNode.setNodeType(NodeTypes.CURSOR_NODE);
        bindAndTransform(cursorNode);
        copyExposedNames(fromSubquery.getResultColumns(), fromSubquery.getSubquery().getResultColumns());
        fromSubquery.setResultColumns(fromSubquery.getSubquery().getResultColumns());

        PlanContext plan = new PlanContext(this);
        plan.setPlan(new AST(cursorNode, null));

        // can't user OperatorCompiler.compile, because it expects to return BasePlannable
        ASTStatementLoader stmtLoader = new ASTStatementLoader();
        stmtLoader.apply(plan);

        TypeResolver typeResolver = new TypeResolver();
        typeResolver.apply(plan);

        copyTypes((ResultSet) ((SelectQuery)plan.getPlan()).getInput(), fromSubquery.getResultColumns());
        
    }

    protected void copyExposedNames(ResultColumnList fromList, ResultColumnList toList) {
        int i = 0;
        if (fromList != null) {
            for (ResultColumn column : toList) {
                column.setName(fromList.get(i).getName());
                i++;
            }
        }
    }

    protected void copyTypes(ResultSet fromList, ResultColumnList toList) {
        int i = 0;
        for (ResultColumn column : toList) {
            try {
                TInstance fieldType = fromList.getFields().get(i).getType();
                if (fieldType != null)
                    column.setType(fromList.getFields().get(i).getType().dataTypeDescriptor());
            } catch (StandardException e) {
                throw new SQLParserInternalException(e);
            }
            i++;
        }
    }
}
