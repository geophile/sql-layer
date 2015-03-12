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

import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.service.TypesRegistry;

/** Add convenience methods for setting up tests with columns typed by name. */
public class TestAISBuilder extends AISBuilder
{
    private final TypesRegistry typesRegistry;

    public TestAISBuilder(TypesRegistry typesRegistry) {
        this.typesRegistry = typesRegistry;
    }

    public TestAISBuilder(AkibanInformationSchema ais, TypesRegistry typesRegistry) {
        super(ais);
        this.typesRegistry = typesRegistry;
    }
    
    public TestAISBuilder(AkibanInformationSchema ais, NameGenerator nameGenerator,
                          TypesRegistry typesRegistry, StorageFormatRegistry storageFormatRegistry) {
        super(ais, nameGenerator, storageFormatRegistry);
        this.typesRegistry = typesRegistry;
    }

    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         boolean nullable) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               null, null, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         boolean nullable, boolean autoincrement) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               null, null, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, autoincrement, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, Long typeParameter2, boolean nullable) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, typeParameter2, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, boolean nullable,
                         String charset, String collation) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, null, charset, collation, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, null, null);
    }
    
    public Column column(String schemaName, String tableName, String columnName,
                         Integer position, String typeBundle, String typeName,
                         Long typeParameter1, Long typeParameter2, boolean nullable,
                         String defaultValue, String defaultFunction) {
        TInstance type = typesRegistry.getType(typeBundle, typeName,
                                               typeParameter1, typeParameter2, nullable,
                                               schemaName, tableName, columnName);
        return column(schemaName, tableName, columnName, position,
                      type, false, defaultValue, defaultFunction);
    }
    
    public void parameter(String schemaName, String routineName, 
                          String parameterName, Parameter.Direction direction, 
                          String typeBundle, String typeName,
                          Long typeParameter1, Long typeParameter2) {
        TInstance type = typesRegistry.getType(typeBundle, typeName, typeParameter1, typeParameter2, true, schemaName, routineName, parameterName);
        parameter(schemaName, routineName, parameterName, direction, type);
    }

}
