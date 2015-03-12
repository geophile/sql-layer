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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.server.store.format.FullTextIndexFileStorageDescription;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.util.Set;

public class FullTextIndexShared implements CacheValueGenerator<FullTextIndexInfo>, Closeable
{
    private final IndexName name;
    private File path;
    private Set<String> casePreservingFieldNames;
    private String defaultFieldName;
    private Directory directory;
    private Analyzer analyzer;
    private StandardQueryParser parser;
    private Indexer indexer;
    private Searcher searcher;

    public FullTextIndexShared(IndexName name) {
        this.name = name;
    }

    public FullTextIndexInfo init(AkibanInformationSchema ais, final FullTextIndexInfo info, 
                                  File basepath) {
        FullTextIndexFileStorageDescription storage = (FullTextIndexFileStorageDescription)info.getIndex().getStorageDescription();
        path = storage.mergePath(basepath);
        casePreservingFieldNames = info.getCasePreservingFieldNames();
        defaultFieldName = info.getDefaultFieldName();
        // Put into cache.
        return ais.getCachedValue(this, new CacheValueGenerator<FullTextIndexInfo>() {
                                      @Override
                                      public FullTextIndexInfo valueFor(AkibanInformationSchema ais) {
                                          return info;
                                      }
                                  });
    }

    public IndexName getName() {
        return name;
    }

    public File getPath() {
        return path;
    }

    public Set<String> getCasePreservingFieldNames() {
        return casePreservingFieldNames;
    }

    public String getDefaultFieldName() {
        return defaultFieldName;
    }

    public synchronized Directory open() throws IOException {
        if (directory == null) {
            directory = FSDirectory.open(path);
        }
        return directory;
    }

    @Override
    public synchronized void close() throws IOException {
        if (indexer != null) {
            indexer.close();
            indexer = null;
        }
        if (searcher != null) {
            searcher.close();
            searcher = null;
        }
        if (directory != null) {
            directory.close();
            directory = null;
        }
    }

    public FullTextIndexInfo forAIS(AkibanInformationSchema ais) {
        return ais.getCachedValue(this, this);
    }

    @Override
    public FullTextIndexInfo valueFor(AkibanInformationSchema ais) {
        FullTextIndexInfo result = new FullTextIndexInfo(this);
        result.init(ais);
        return result;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public StandardQueryParser getParser() {
        return parser;
    }

    public void setParser(StandardQueryParser parser) {
        this.parser = parser;
    }

    public Indexer getIndexer() {
        return indexer;
    }

    public void setIndexer(Indexer indexer) {
        this.indexer = indexer;
    }

    public Searcher getSearcher() {
        return searcher;
    }

    public void setSearcher(Searcher searcher) {
        this.searcher = searcher;
    }

}
