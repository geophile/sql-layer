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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.Visitor;
import com.foundationdb.server.error.AISNullReferenceException;
import com.foundationdb.server.error.BadAISReferenceException;

/**
 * Validates the internal references used by the AIS are correct. 
 * @author tjoneslo
 *
 */
class ReferencesCorrect implements AISValidation {
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ReferenceVisitor visitor = new ReferenceVisitor(output);
        ais.visit(visitor);
    }

    private static class ReferenceVisitor implements Visitor {
        private final AISValidationOutput output;
        private Table visitingTable = null;
        private Index visitingIndex = null;
        private Group visitingGroup = null;

        public ReferenceVisitor(AISValidationOutput output) {
            this.output = output;
        }

        @Override
        public void visit(Group group) {
            visitingGroup = group;
            if (group == null) {
                output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("ais", "", "group")));
            } else if (group.getRoot() == null) {
                output.reportFailure(new AISValidationFailure(
                    new AISNullReferenceException("group", group.getName().toString(), "root table")));
            }
        }

        @Override
        public void visit(Table table) {
            visitingTable = table;
            if (table == null) {
                output.reportFailure(new AISValidationFailure(
                        new AISNullReferenceException ("ais", "", "user table")));
            }
        }

        @Override
        public void visit(Column column) {
            if (column == null) {
                output.reportFailure(new AISValidationFailure(
                        new AISNullReferenceException ("user table", visitingTable.getName().toString(), "column")));
            } else if (column.getTable() != visitingTable) {
                output.reportFailure(new AISValidationFailure(
                        new BadAISReferenceException ("column", column.getName(), "table", visitingTable.getName().toString())));
            }
        }

        @Override
        public void visit(Index index) {
            visitingIndex = index;
            if (index == null) {
                output.reportFailure(new AISValidationFailure (
                        new AISNullReferenceException ("table", visitingTable.getName().toString(), "index")));
            } else if (index.isTableIndex() && index.rootMostTable() != visitingTable) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Table index", index.getIndexName().toString(),
                                                      "table", visitingTable.getName().toString())));
            } else if (index.isGroupIndex() && ((GroupIndex)index).getGroup() != visitingGroup) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Group index", index.getIndexName().toString(),
                                                      "group", visitingGroup.getName().toString())));
            }
        }

        @Override
        public void visit(IndexColumn indexColumn) {
            if (indexColumn == null) {
                output.reportFailure(new AISValidationFailure (
                        new AISNullReferenceException ("index", visitingIndex.getIndexName().toString(), "column")));
            } else if (indexColumn.getIndex() != visitingIndex) {
                output.reportFailure(new AISValidationFailure (
                        new BadAISReferenceException ("Index column",indexColumn.getColumn().getName(),
                                                      "index", visitingIndex.getIndexName().toString())));
            }
        }
    }
}
