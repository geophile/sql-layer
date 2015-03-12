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
package com.foundationdb.rest.dml;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.TableName;
import com.fasterxml.jackson.databind.JsonNode;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.foundationdb.server.service.session.Session;

public interface RestDMLService {
    public void getAllEntities(PrintWriter writer, TableName tableName, Integer depth);
    public void getEntities(PrintWriter writer, TableName tableName, Integer depth, String pks);
    public void insert(PrintWriter writer, TableName tableName, JsonNode node);
    public void delete(TableName tableName, String pks);
    public void update(PrintWriter writer, TableName tableName, String values, JsonNode node);
    public void upsert(PrintWriter writer, TableName tableName, JsonNode node);

    public void insertNoTxn(Session session, PrintWriter writer, TableName tableName, JsonNode node);
    public void updateNoTxn(Session session, PrintWriter writer, TableName tableName, String values, JsonNode node);

    public void runSQL(PrintWriter writer, HttpServletRequest request, String sql, String schema) throws SQLException;
    public void runSQL(PrintWriter writer, HttpServletRequest request, List<String> sql) throws SQLException;
    public void runSQLParameter(PrintWriter writer,HttpServletRequest request, String SQL, List<String> parameters) throws SQLException;
    public void explainSQL(PrintWriter writer, HttpServletRequest request, String sql) throws IOException, SQLException;

    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                              TableName procName, Map<String,List<String>> queryParams, String content) throws SQLException;

    public void fullTextSearch(PrintWriter writer, IndexName indexName, Integer depth, String query, Integer limit);
}
