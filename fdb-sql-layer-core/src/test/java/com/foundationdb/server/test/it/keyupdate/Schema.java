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
package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.rowtype.RowType;

public class Schema
{
    // For all KeyUpdate*IT
    static Integer vendorId;
    static RowType vendorRT;
    static Integer customerId;
    static RowType customerRT;
    static Integer orderId;
    static RowType orderRT;
    static Integer itemId;
    static RowType itemRT;
    static Group group;
    // For KeyUpdateIT and KeyUpdateCascadingKeysIT
    static Integer v_vid;
    static Integer v_vx;
    static Integer c_cid;
    static Integer c_vid;
    static Integer c_cx;
    static Integer o_oid;
    static Integer o_cid;
    static Integer o_vid;
    static Integer o_ox;
    static Integer o_priority;
    static Integer o_when;
    static Integer i_vid;
    static Integer i_cid;
    static Integer i_oid;
    static Integer i_iid;
    static Integer i_ix;
    // For MultiColumnKeyUpdateIT and MultiColumnKeyUpdateCascadingKeysIT
    static Integer v_vid1;
    static Integer v_vid2;
    static Integer c_vid1;
    static Integer c_vid2;
    static Integer c_cid1;
    static Integer c_cid2;
    static Integer o_vid1;
    static Integer o_vid2;
    static Integer o_cid1;
    static Integer o_cid2;
    static Integer o_oid1;
    static Integer o_oid2;
    static Integer i_vid1;
    static Integer i_vid2;
    static Integer i_cid1;
    static Integer i_cid2;
    static Integer i_oid1;
    static Integer i_oid2;
    static Integer i_iid1;
    static Integer i_iid2;
}
