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
package com.foundationdb.blob;

import com.foundationdb.*;
import com.foundationdb.async.*;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple2;

public class SQLBlob extends BlobAsync {
    
    public SQLBlob(Subspace subspace){
        super (subspace);
    }

    public Future<Void> setLinkedTable(TransactionContext tcx, final int tableId) {
        return tcx.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tr) {
                tr.set(attributeKey(), Tuple2.from((long)tableId).pack());
                return new ReadyFuture<>((Void) null);
            }
        });
    }
    
    public Future<Integer> getLinkedTable(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Integer>>() {
            @Override
            public Future<Integer> apply(Transaction tr) {
                return tr.get(attributeKey()).map(new Function<byte[], Integer>() {
                    @Override
                    public Integer apply(byte[] tableIdBytes) {
                        if(tableIdBytes == null) {
                            return null;
                        }
                        int tableId = (int)Tuple2.fromBytes(tableIdBytes).getLong(0);
                        return Integer.valueOf(tableId);
                    }
                });
            }
        });
    }

    public Future<Boolean> isLinked(TransactionContext tcx) {
        return tcx.runAsync(new Function<Transaction, Future<Boolean>>() {
            @Override
            public Future<Boolean> apply(Transaction tr) {
                return tr.get(attributeKey()).map(new Function<byte[], Boolean>() {
                    @Override
                    public Boolean apply(byte[] tableIdBytes) {
                        if(tableIdBytes == null) {
                            return false;
                        }
                        return true;
                    }
                });
            }
        });
    }    
}
