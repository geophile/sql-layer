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

package com.foundationdb.server.service.blob;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.qp.operator.QueryContext;

import java.util.UUID;

public interface LobService {
    public void createNewLob(Session session, UUID lobId);
    public boolean existsLob(Session session, UUID lobId);
    public void deleteLob(Session session, UUID lobId);
    public void linkTableBlob(Session session, UUID lobId, int tableId);
    public long sizeBlob(Session session, UUID lobId);
    public byte[] readBlob(Session session, UUID lobId, long offset, int length);
    public byte[] readBlob(Session session, UUID lobId);
    public void writeBlob(Session session, UUID lobId, long offset, byte[] data);
    public void appendBlob(Session session, UUID lobId, byte[] data);
    public void truncateBlob(Session session, UUID lobId, long size);
    public void clearAllLobs(Session session);
    public void verifyAccessPermission(Session session, QueryContext context, UUID lobId);

}



