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

import java.util.Iterator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.error.JoinColumnMismatchException;
import com.foundationdb.server.error.JoinParentNoExplicitPK;
import com.foundationdb.server.error.JoinToWrongColumnsException;

class JoinToParentPK implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            
            // bug 931258: If parent has no external PK, flag this as an error. 
            if (join.getParent().getPrimaryKey() == null) {
                output.reportFailure(new AISValidationFailure(
                        new JoinParentNoExplicitPK (join.getParent().getName())));
                continue;
            }
            TableIndex parentPK= join.getParent().getPrimaryKey().getIndex();
            if (parentPK.getKeyColumns().size() != join.getJoinColumns().size()) {
                output.reportFailure(new AISValidationFailure(
                        new JoinColumnMismatchException (join.getJoinColumns().size(),
                                join.getChild().getName(),
                                join.getParent().getName(),
                                parentPK.getKeyColumns().size())));

                continue;
            }
            Iterator<JoinColumn>  joinColumns = join.getJoinColumns().iterator();
            for (IndexColumn parentPKColumn : parentPK.getKeyColumns()) {
                JoinColumn joinColumn = joinColumns.next();
                if (parentPKColumn.getColumn() != joinColumn.getParent()) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinToWrongColumnsException (
                                    join.getChild().getName(), 
                                    joinColumn.getParent().getName(), 
                                    parentPK.getTable().getName(), parentPKColumn.getColumn().getName())));
                }
            }
        }
    }
}
