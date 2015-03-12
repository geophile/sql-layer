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
import com.foundationdb.ais.model.Join;
import com.foundationdb.server.error.GroupMultipleVirtualTables;

/**
 * Validate the current assumption of groups with a virtual table contain only one
 * table, that is, there is no muti-table groups with the virtual tables.
 * (The VirtualTablesNotMixed validation ensures virtual tables are not mixed with other
 *  types of tables). 
 *  TODO: It would be nice to remove this limitation of the current system. 
 * @author tjoneslo
 *
 */
public class VirtualTableSingleTableGroup implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Join join : ais.getJoins().values()) {
            if (join.getChild().isVirtual()) {
                output.reportFailure(new AISValidationFailure (
                        new GroupMultipleVirtualTables(join.getParent().getName(), join.getChild().getName())));
            }
        }
    }

}
