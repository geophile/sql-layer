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
package com.foundationdb.server.test.mt;

import com.foundationdb.qp.row.Row;
import org.junit.Test;

/** Interleaved DML during an ALTER ADD FOREIGN KEY ON UPDATE SET NULL ON DELETE CASCADE. */
public class OnlineAlterAddForeignKeyNullCascadeMT extends OnlineAlterAddForeignKeyCascadeNullMT
{
    protected static final String ALTER_ADD_FK = "ALTER TABLE "+CHILD_TABLE+" ADD CONSTRAINT fk1 FOREIGN KEY(pid) "+
                                                 "REFERENCES "+PARENT_TABLE+"(pid) ON UPDATE SET NULL ON DELETE CASCADE";

    @Override
    protected String getDDL() {
        return ALTER_ADD_FK;
    }

    // Note: Not actually violations with CASCADE or SET NULL

    @Override
    @Test
    public void updateViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlViolationPostMetaToPreFinal(updateCreator(pID, oldRow, newRow),
                                       replace(parentGroupRows, 1, newRow),
                                       replace(childGroupRows, 1, testRow(childRowType, 20, null)),
                                       true);
    }

    @Override
    @Test
    public void deleteViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlViolationPostMetaToPreFinal(deleteCreator(pID, oldRow),
                                       remove(parentGroupRows, 1),
                                       remove(childGroupRows, 1),
                                       true);
    }
}
