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

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.GeneratorWrongDatatypeException;
import com.foundationdb.server.error.UnsupportedColumnDataTypeException;
import com.foundationdb.server.error.UnsupportedIndexDataTypeException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypeValidator;

class SupportedColumnTypes implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ColumnTypeVisitor visitor = new ColumnTypeVisitor(output, ais);
        ais.visit(visitor);
    }

    private static class ColumnTypeVisitor extends AbstractVisitor {
        private final AISValidationOutput failures;
        private final AkibanInformationSchema sourceAIS;

        private ColumnTypeVisitor(AISValidationOutput failures, AkibanInformationSchema sourceAIS) {
            this.failures = failures;
            this.sourceAIS = sourceAIS;
        }

        @Override
        public void visit(Column column) {
            if (!TypeValidator.isSupportedForColumn(column.getType())) {
                failures.reportFailure(new AISValidationFailure (
                        new UnsupportedColumnDataTypeException(column.getTable().getName(),
                                column.getName(), column.getTypeName())));
            }
            
            if (column.getDefaultIdentity() != null && !TypeValidator.isSupportedForGenerator(column.getType())) {
                failures.reportFailure(new AISValidationFailure(
                        new GeneratorWrongDatatypeException (column.getTable().getName(),
                                column.getName(), column.getTypeName())));
            }
        }

        @Override
        public void visit(IndexColumn indexColumn) {
            Index index = indexColumn.getIndex();
            TInstance columnType = indexColumn.getColumn().getType();
            int columnPosition = indexColumn.getPosition();
            boolean columnTypeOK;
            if (index.isSpatial() &&
                columnPosition >= index.firstSpatialArgument() &&
                columnPosition <= index.lastSpatialArgument()) {
                if (index.firstSpatialArgument() == index.lastSpatialArgument()) {
                    // Non-point spatial index
                    columnTypeOK = TypeValidator.isSupportedForNonPointSpatialIndex(columnType);
                } else {
                    columnTypeOK = TypeValidator.isSupportedForIndex(columnType);
                }
            } else {
                columnTypeOK = TypeValidator.isSupportedForIndex(columnType);
            }
            if (!columnTypeOK) {
                failures.reportFailure(new AISValidationFailure (
                        new UnsupportedIndexDataTypeException (
                                new TableName (index.getIndexName().getSchemaName(),
                                index.getIndexName().getTableName()),
                                index.getIndexName().getName(),
                                indexColumn.getColumn().getName(),
                                indexColumn.getColumn().getTypeName())));
            }
        }
    }
}
