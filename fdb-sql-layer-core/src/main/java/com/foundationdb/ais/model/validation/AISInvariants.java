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
package com.foundationdb.ais.model.validation;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.error.DuplicateConstraintNameException;
import com.foundationdb.server.error.AISNullReferenceException;
import com.foundationdb.server.error.DuplicateColumnNameException;
import com.foundationdb.server.error.DuplicateGroupNameException;
import com.foundationdb.server.error.DuplicateIndexColumnException;
import com.foundationdb.server.error.DuplicateIndexException;
import com.foundationdb.server.error.DuplicateParameterNameException;
import com.foundationdb.server.error.DuplicateRoutineNameException;
import com.foundationdb.server.error.DuplicateSequenceNameException;
import com.foundationdb.server.error.DuplicateSQLJJarNameException;
import com.foundationdb.server.error.DuplicateTableNameException;
import com.foundationdb.server.error.JoinToProtectedTableException;
import com.foundationdb.server.error.NameIsNullException;

public class AISInvariants {

    public static void checkNullField (Object field, String owner, String fieldName, String reference) {
        if (field == null) {
            throw new AISNullReferenceException(owner, fieldName, reference);
        }
    }
    
    public static void checkNullName (final String name, final String source, final String type) {
        if (name == null || name.length() == 0) {
            throw new NameIsNullException (source, type);
        }
    }
    
    public static void checkDuplicateTables(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        if (ais.getColumnar(schemaName, tableName) != null) {
            throw new DuplicateTableNameException (new TableName(schemaName, tableName));
        }
    }
    
    public static void checkDuplicateSequence(AkibanInformationSchema ais, String schemaName, String sequenceName)
    {
        if (ais.getSequence(new TableName (schemaName, sequenceName)) != null) {
            throw new DuplicateSequenceNameException (new TableName(schemaName, sequenceName));
        }
    }
    
    public static void checkDuplicateColumnsInTable(Columnar table, String columnName)
    {
        if (table.getColumn(columnName) != null) {
            throw new DuplicateColumnNameException(table.getName(), columnName);
        }
    }
    public static void checkDuplicateColumnPositions(Columnar table, Integer position) {
        if (position < table.getColumns().size() &&
                table.getColumn(position) != null &&
                table.getColumn(position).getPosition().equals(position)) {
            throw new DuplicateColumnNameException (table.getName(), table.getColumn(position).getName());
        }
    }
    
    public static void checkDuplicateColumnsInIndex(Index index, TableName columnarName, String columnName)
    {
        int firstSpatialInput = Integer.MAX_VALUE;
        int lastSpatialInput = Integer.MIN_VALUE;
        if (index.isSpatial()) {
            firstSpatialInput = index.firstSpatialArgument();
            lastSpatialInput = firstSpatialInput + index.spatialColumns() - 1;
        }
        for(IndexColumn icol : index.getKeyColumns()) {
            int indexColumnPosition = icol.getPosition();
            if (indexColumnPosition < firstSpatialInput || indexColumnPosition > lastSpatialInput) {
                Column column = icol.getColumn();
                if (column.getName().equals(columnName) && column.getColumnar().getName().equals(columnarName)) {
                    throw new DuplicateIndexColumnException (index, columnName);
                }
            }
        }
    }
    
    public static void checkDuplicateIndexesInTable(Table table, String indexName)
    {
        if (isIndexInTable(table, indexName)) {
            throw new DuplicateIndexException (table.getName(), indexName);
        }
    }
    
    public static void checkDuplicateIndexesInGroup(Group group, String indexName)
    {
        if (group.getIndex(indexName) != null) {
            throw new DuplicateIndexException (group.getName(), indexName);
        }
    }

    public static boolean isIndexInTable (Table table, String indexName)
    {
        return table.getIndex(indexName) != null || table.getFullTextIndex(indexName) != null;
    }
 
    public static void checkDuplicateIndexColumnPosition (Index index, Integer position) {
        if (position < index.getKeyColumns().size()) {
            // TODO: Index uses position for a relative ordering, not an absolute position. 
        }
    }
    public static void checkDuplicateGroups (AkibanInformationSchema ais, TableName groupName)
    {
        if (ais.getGroup(groupName) != null) {
            throw new DuplicateGroupNameException(groupName);
        }
    }    

    public static void checkDuplicateRoutine(AkibanInformationSchema ais, String schemaName, String routineName)
    {
        if (ais.getRoutine(new TableName(schemaName, routineName)) != null) {
            throw new DuplicateRoutineNameException(new TableName(schemaName, routineName));
        }
    }
    
    public static void checkDuplicateParametersInRoutine(Routine routine, String parameterName, Parameter.Direction direction)
    {
        if (direction == Parameter.Direction.RETURN) {
            if (routine.getReturnValue() != null) {
                throw new DuplicateParameterNameException(routine.getName(), "return value");
            }
        }
        else {
            if (routine.getNamedParameter(parameterName) != null) {
                throw new DuplicateParameterNameException(routine.getName(), parameterName);
            }
        }
    }

    public static void checkDuplicateSQLJJar(AkibanInformationSchema ais, String schemaName, String jarName)
    {
        if (ais.getSQLJJar(new TableName(schemaName, jarName)) != null) {
            throw new DuplicateSQLJJarNameException(new TableName(schemaName, jarName));
        }
    }
    
    public static void checkDuplicateConstraintsInSchema(AkibanInformationSchema ais, TableName constraintName) {
        if (constraintName != null) {
            Schema schema = ais.getSchema(constraintName.getSchemaName());
            if (schema != null && schema.hasConstraint(constraintName.getTableName())) {
                throw new DuplicateConstraintNameException(constraintName);
            }
        }
    }
    
    public static void checkJoinTo(Join join, TableName childName, boolean isInternal) {
        TableName parentName = (join != null) ? join.getParent().getName() : null;
        if(parentName != null) {
            boolean inAIS = parentName.inSystemSchema();
            if(inAIS && !isInternal) {
                throw new JoinToProtectedTableException(parentName, childName);
            } else if(!inAIS && isInternal) {
                throw new IllegalArgumentException("Internal table join to non-IS table: " + childName);
            }
        }
    }
    
}
