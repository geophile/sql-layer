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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Set;

public class SelectiveCaseAnalyzer extends Analyzer
{
    protected static final Version matchVersion = Version.LUCENE_40;
    
    private final Set<String> casePreservingFieldNames;

    static class CaseReuseStrategy extends ReuseStrategy {
        private final Set<String> casePreservingFieldNames;

        public CaseReuseStrategy(Set<String> casePreservingFieldNames) {
            this.casePreservingFieldNames = casePreservingFieldNames;
        }

        @Override
        public TokenStreamComponents getReusableComponents(String fieldName) {
            TokenStreamComponents[] stored = (TokenStreamComponents[])getStoredValue();
            if (stored == null) {
                return null;
            }
            else if (casePreservingFieldNames.contains(fieldName)) {
                return stored[0];
            }
            else {
                return stored[1];
            }
        }

        @Override
        public void setReusableComponents(String fieldName, TokenStreamComponents components) {
            TokenStreamComponents[] stored = (TokenStreamComponents[])getStoredValue();
            if (stored == null) {
                stored = new TokenStreamComponents[2];
                setStoredValue(stored);
            }
            if (casePreservingFieldNames.contains(fieldName)) {
                stored[0] = components;
            }
            else {
                stored[1] = components;
            }
        }
    }

    public SelectiveCaseAnalyzer(Set<String> casePreservingFieldNames) {
        super(new CaseReuseStrategy(casePreservingFieldNames));
        this.casePreservingFieldNames = casePreservingFieldNames;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        final Tokenizer source = new StandardTokenizer(matchVersion, reader);
        TokenStream sink = new StandardFilter(matchVersion, source);
        if (!casePreservingFieldNames.contains(fieldName)) {
            sink = new LowerCaseFilter(matchVersion, sink);
        }
        sink = new StopFilter(matchVersion, sink, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        return new TokenStreamComponents(source, sink);
    }    

}
