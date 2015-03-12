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
package com.foundationdb.server.service.text;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;

public class Indexer implements Closeable
{
    private final FullTextIndexShared index;
    private final IndexWriter writer;
    
    public Indexer(FullTextIndexShared index, Analyzer analyzer) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        iwc.setMaxBufferedDeleteTerms(1); // The deletion needs to be reflected immediately (on disk)
        this.index = index;
        this.writer = new IndexWriter(index.open(),  iwc);

    }

    public FullTextIndexShared getIndex() {
        return index;
    }

    public IndexWriter getWriter() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
