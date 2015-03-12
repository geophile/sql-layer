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

import com.foundationdb.server.service.transaction.*;
import com.foundationdb.server.store.FDBTransactionService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.Transaction;
import java.util.UUID;

import org.junit.*;


public class LobServiceIT extends ITBase {
    private LobService ls;
    private UUID idA;
    private UUID idB;
    private final byte[] data = "foo".getBytes();
    
    @Test
    public void utilizeLobService() {
        // registration
        Assert.assertNotNull(ls);

        // blob creation
        UUID id = UUID.randomUUID();
        getOrBeginTransaction();
        ls.createNewLob(session(), id);
        ls.appendBlob(session(), id, data);
        Assert.assertEquals(ls.sizeBlob(session(), id), new Long(data.length).longValue());
        
        // data retrieval
        byte[] output = ls.readBlob(session(), id, 0, data.length);
        Assert.assertArrayEquals(data, output);
        output = ls.readBlob(session(), id);
        Assert.assertArrayEquals(data, output);
        
        ls.truncateBlob(session(), id, 2L);
        Assert.assertEquals(ls.sizeBlob(session(), id), 2L);
        ls.truncateBlob(session(), id, 0L);
        Assert.assertEquals(ls.sizeBlob(session(), id), 0L);
        ls.appendBlob(session(), id, data);
        output = ls.readBlob(session(), id);
        Assert.assertArrayEquals(data, output);        
        
        ls.deleteLob(session(), id);
        Assert.assertFalse(ls.existsLob(session(), id));
        ls.createNewLob(session(), id);
        commit();
    }
    
    @Before
    public void setUp(){
        // registration
        this.ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertNotNull(ls);
        getOrBeginTransaction();
        idA = UUID.randomUUID();
        idB = UUID.randomUUID();
        ls.createNewLob(session(), idA);
        ls.createNewLob(session(), idB);
        ls.linkTableBlob(session(), idA, 1);
        commit();
    }
    
    @Test
    public void checkBlobCleanUp() {
        getOrBeginTransaction();
        Assert.assertTrue(ls.existsLob(session(), idA));
        Assert.assertFalse(ls.existsLob(session(), idB));
        commit();
    }
    
    
    private Transaction getOrBeginTransaction() {
        TransactionService txnService = txnService();
        if (txnService instanceof FDBTransactionService) {
            if ( txnService.isTransactionActive(session())) {
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            } else {
                txnService.beginTransaction(session());
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            }
        }
        else 
            return null;
    }
    
    private void commit() {
        TransactionService ts = txnService();
        ts.commitTransaction(session());
        ts.rollbackTransactionIfOpen(session());
    }
}
