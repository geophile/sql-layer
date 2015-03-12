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

import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.InvalidSQLJDeploymentDescriptorException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.aisddl.AISDDL;
import com.foundationdb.sql.aisddl.DDLHelper;
import com.foundationdb.sql.parser.CreateAliasNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DropAliasNode;
import com.foundationdb.sql.parser.SQLParserException;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

public class SQLJJarDeployer
{
    public static final String MANIFEST_ATTRIBUTE = "SQLJDeploymentDescriptor";
    public static final String DESCRIPTOR_FILE = "\\s*SQLActions\\s*\\[\\s*\\]\\s*\\=\\s*\\{.*\\}\\s*";
    public static final String BEGIN_INSTALL = "BEGIN INSTALL";
    public static final String END_INSTALL = "END INSTALL";
    public static final String BEGIN_REMOVE = "BEGIN REMOVE";
    public static final String END_REMOVE = "END REMOVE";

    private ServerQueryContext context;
    private TableName jarName;
    private ServerSession server;

    public SQLJJarDeployer(ServerQueryContext context, TableName jarName) {
        this.context = context;
        this.jarName = jarName;
        server = context.getServer();
    }

    public void deploy() {
        loadDeploymentDescriptor(false);
    }
   
    public void undeploy() {
        loadDeploymentDescriptor(true);
    }

    private void loadDeploymentDescriptor(boolean undeploy) {
        if (jarName == null) return;
        try (JarFile jarFile = server.getRoutineLoader().openSQLJJarFile(context.getSession(), jarName)) {
            Manifest manifest = jarFile.getManifest();
            for (Map.Entry<String,Attributes> entry : manifest.getEntries().entrySet()) {
                String val = entry.getValue().getValue(MANIFEST_ATTRIBUTE);
                if ((val != null) && Boolean.parseBoolean(val)) {
                    JarEntry jarEntry = jarFile.getJarEntry(entry.getKey());
                    if (jarEntry != null) {
                        InputStream istr = jarFile.getInputStream(jarEntry);
                        loadDeploymentDescriptor(istr, undeploy);
                        break;
                    }
                }
            }
        }
        catch (IOException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
    }

    private void loadDeploymentDescriptor(InputStream istr, boolean undeploy) throws IOException {
        StringBuilder contents = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(istr, "UTF-8"));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            contents.append(line).append('\n');
        }
        if (!Pattern.compile(DESCRIPTOR_FILE, Pattern.DOTALL).matcher(contents).matches())
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Incorrect file format");
        String header, footer;
        if (undeploy) {
            header = BEGIN_REMOVE;
            footer = END_REMOVE;
        }
        else {
            header = BEGIN_INSTALL;
            footer = END_INSTALL;
        }
        int start = contents.indexOf(header);
        if (start < 0)
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Actions not found");
        start += header.length();
        int end = contents.indexOf(footer, start);
        if (end < 0)
            throw new InvalidSQLJDeploymentDescriptorException(jarName, "Actions not terminated");
        String sql = contents.substring(start, end);
        List<StatementNode> stmts;
        try {
            stmts = server.getParser().parseStatements(sql);
        } 
        catch (SQLParserException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        catch (StandardException ex) {
            throw new InvalidSQLJDeploymentDescriptorException(jarName, ex);
        }
        int nstmts = stmts.size();
        List<DDLStatementNode> ddls = new ArrayList<>(nstmts);
        List<String> sqls = new ArrayList<>(nstmts);
        for (StatementNode stmt : stmts) {
            boolean stmtOkay = false, thisjarOkay = false;
            if (undeploy) {
                if (stmt instanceof DropAliasNode) {
                    DropAliasNode dropAlias = (DropAliasNode)stmt;
                    switch (dropAlias.getAliasType()) {
                    case PROCEDURE:
                    case FUNCTION:
                        stmtOkay = true;
                        {
                            TableName routineName = DDLHelper.convertName(server.getDefaultSchemaName(), dropAlias.getObjectName());
                            Routine routine = server.getAIS().getRoutine(routineName);
                            if (routine != null) {
                                SQLJJar sqljjar = routine.getSQLJJar();
                                thisjarOkay = ((sqljjar != null) && 
                                               jarName.equals(sqljjar.getName()));
                            }
                        }
                        break;
                    }
                }
            }
            else {
                if (stmt instanceof CreateAliasNode) {
                    CreateAliasNode createAlias = (CreateAliasNode)stmt;
                    switch (createAlias.getAliasType()) {
                    case PROCEDURE:
                    case FUNCTION:
                        stmtOkay = true;
                        if ((createAlias.getJavaClassName() != null) &&
                            createAlias.getJavaClassName().startsWith("thisjar:")) {
                            createAlias.setUserData(jarName);
                            thisjarOkay = true;
                        }
                        break;
                    }
                }
            }
            if (!stmtOkay)
                throw new InvalidSQLJDeploymentDescriptorException(jarName, "Statement not allowed " + stmt.statementToString());
            if (!thisjarOkay)
                throw new InvalidSQLJDeploymentDescriptorException(jarName, "Must refer to thisjar:");
            ddls.add((DDLStatementNode)stmt);
            sqls.add(sql.substring(stmt.getBeginOffset(), stmt.getEndOffset() + 1));
        }
        for (int i = 0; i < nstmts; i++) {
            AISDDL.execute(ddls.get(i), sqls.get(i), context);
        }
    }
}
