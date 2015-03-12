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
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.server.error.DuplicateIndexIdException;
import com.foundationdb.server.error.InvalidIndexIDException;

import java.util.Map;
import java.util.HashMap;

/**
 * Index IDs must be:
 * 1) Unique across all indexes within the group
 * 2) Greater than 0 (zero is reserved for HKey scan in DML API, arrays sized to max ID)
 */
public class IndexIDValidation implements AISValidation
{
    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        ais.visit(new IndexIDVisitor(output));
    }

    private static class IndexIDVisitor extends AbstractVisitor
    {
        private final Map<Integer,Index> current = new HashMap<>();
        private final AISValidationOutput failures;

        private IndexIDVisitor(AISValidationOutput failures) {
            this.failures = failures;
        }

        @Override
        public void visit(Group group) {
            current.clear();
        }

        @Override
        public void visit(Index index) {
            Index prev = current.put(index.getIndexId(), index);
            IndexName name = index.getIndexName();
            if(prev != null) {
                failures.reportFailure(new AISValidationFailure(new DuplicateIndexIdException(prev.getIndexName(), name)));
            }
            Integer indexID = index.getIndexId();
            if((indexID == null) || (indexID <= 0)) {
                failures.reportFailure(new AISValidationFailure(new InvalidIndexIDException(name, indexID)));
            }
        }
    }
}
