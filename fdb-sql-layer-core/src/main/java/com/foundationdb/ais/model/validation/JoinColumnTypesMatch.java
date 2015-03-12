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
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.server.error.JoinColumnTypesMismatchException;
import com.foundationdb.server.types.common.types.TypeValidator;

/**
 * validate the columns used for the join in the parent (PK) and 
 * the child are join-able, according to the AIS.
 * @author tjoneslo
 *
 */
public class JoinColumnTypesMatch implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            if (join.getParent().getPrimaryKey() == null) {
                //bug 931258: Attempting to join to a table without a explicit PK,
                // causes getJoinColumns to throw an JoinParentNoExplicitPK exception. 
                // This is explicitly validated in JoinToParentPK
                continue;
            }
            for (JoinColumn column : join.getJoinColumns()) {
                Column parentCol = column.getParent();
                Column childCol = column.getChild();
                if (!TypeValidator.isSupportedForJoin(parentCol.getType(), childCol.getType())) {
                    output.reportFailure(new AISValidationFailure (
                            new JoinColumnTypesMismatchException (parentCol.getTable().getName(), parentCol.getName(),
                                    childCol.getTable().getName(), childCol.getName())));
                }
            }
        }
    }
}
