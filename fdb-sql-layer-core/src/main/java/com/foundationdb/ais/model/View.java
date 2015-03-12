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
package com.foundationdb.ais.model;

import java.util.*;

import com.foundationdb.ais.model.validation.AISInvariants;

public class View extends Columnar
{
    public static View create(AkibanInformationSchema ais,
                              String schemaName, String viewName,
                              String definition, Properties definitionProperties,
                              Map<TableName,Collection<String>> tableColumnReferences) {
        View view = new View(ais, schemaName, viewName);
        view.setDefinition(definition, definitionProperties);
        view.setTableColumnReferences(tableColumnReferences);
        ais.addView(view);
        return view;
    }

    @Override
    public boolean isView() {
        return true;
    }

    public View(AkibanInformationSchema ais, String schemaName, String viewName)
    {
        super(ais, schemaName, viewName);
    }

    public String getDefinition() {
        return definition;
    }

    public Properties getDefinitionProperties() {
        return definitionProperties;
    }

    protected void setDefinition(String definition, Properties definitionProperties) {
        this.definition = definition;
        this.definitionProperties = definitionProperties;
    }

    public Map<TableName,Collection<String>> getTableColumnReferences() {
        return tableColumnReferences;
    }

    protected void setTableColumnReferences(Map<TableName,Collection<String>> tableColumnReferences) {
        this.tableColumnReferences = tableColumnReferences;
    }

    public Collection<TableName> getTableReferences() {
        return tableColumnReferences.keySet();
    }

    public Collection<Column> getTableColumnReferences(Columnar table) {
        Collection<String> colnames = tableColumnReferences.get(table.getName());
        if (colnames == null) return null;
        Collection<Column> columns = new HashSet<>();
        for (String colname : colnames) {
            columns.add(table.getColumn(colname));
        }
        return columns;
    }

    public boolean referencesTable(Columnar table) {
        return tableColumnReferences.containsKey(table.getName());
    }

    public boolean referencesColumn(Column column) {
        Collection<String> entry = tableColumnReferences.get(column.getColumnar().getName());
        return ((entry != null) && entry.contains(column.getName()));
    }

    // State
    private String definition;
    private Properties definitionProperties;
    private Map<TableName,Collection<String>> tableColumnReferences;
}
