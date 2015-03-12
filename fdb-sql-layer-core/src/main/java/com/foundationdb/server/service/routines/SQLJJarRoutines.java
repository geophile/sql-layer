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
package com.foundationdb.server.service.routines;

import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerSession;

/** SQL/J DDL commands are implemented as procedures in the SQLJ schema. */
public class SQLJJarRoutines
{
    private SQLJJarRoutines() {
    }

    public static void install(String url, String jar, long deploy) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName(),
                                                     ddl.getTypesTranslator());
        aisb.sqljJar(jarName).url(url, true);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        ddl.createSQLJJar(server.getSession(), sqljJar);
        if (deploy != 0) {
            new SQLJJarDeployer(context, jarName).deploy();
        }
    }

    public static void replace(String url, String jar) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        NewAISBuilder aisb = AISBBasedBuilder.create(server.getDefaultSchemaName(),
                                                     ddl.getTypesTranslator());
        aisb.sqljJar(jarName).url(url, true);
        SQLJJar sqljJar = aisb.ais().getSQLJJar(jarName);
        ddl.replaceSQLJJar(server.getSession(), sqljJar);
        server.getRoutineLoader().checkUnloadSQLJJar(server.getSession(), jarName);
    }

    public static void remove(String jar, long undeploy) {
        ServerQueryContext context = ServerCallContextStack.getCallingContext();
        ServerSession server = context.getServer();
        TableName jarName = jarName(server, jar);
        DDLFunctions ddl = server.getDXL().ddlFunctions();
        if (undeploy != 0) {
            new SQLJJarDeployer(context, jarName).undeploy();
        }
        ddl.dropSQLJJar(server.getSession(), jarName);
        server.getRoutineLoader().checkUnloadSQLJJar(server.getSession(), jarName);
    }

    private static TableName jarName(ServerSession server, String jar) {
        int idx = jar.lastIndexOf('.');
        if (idx < 0)
            return new TableName(server.getDefaultSchemaName(), jar);
        else
            return new TableName(jar.substring(0, idx), jar.substring(idx+1));
    }
}
