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
package com.foundationdb.server.types;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.util.SparseArray;
import org.joda.time.DateTimeZone;

import java.util.List;

public final class TPreptimeContext {
    public List<TInstance> getInputTypes() {
        return inputTypes;
    }

    public TInstance inputTypeAt(int index) {
        return inputTypes.get(index);
    }

    public TInstance getOutputType() {
        return outputType;
    }

    public void setOutputType(TInstance outputType) {
        this.outputType = outputType;
    }

    public TExecutionContext createExecutionContext() {
        return new TExecutionContext(preptimeCache, inputTypes, outputType, 
                queryContext,
                null, null, null); // TODO pass in
    }
    
    public String getCurrentTimezone()
    {
        // TODO need to get this from the session
        return DateTimeZone.getDefault().getID();
    }
    
    public String getLocale()
    {
        // TODO:
        throw new UnsupportedOperationException("not supported yet");
    }
    
    public Object get(int index) {
        if ((preptimeCache != null) && preptimeCache.isDefined(index))
            return preptimeCache.get(index);
        else
            return null;
    }

    public void set(int index, Object value) {
        if (preptimeCache == null)
            preptimeCache = new SparseArray<>(index);
        preptimeCache.set(index, value);
    }

    public SparseArray<Object> getValues() {
        return preptimeCache;
    }

    public TPreptimeContext(List<TInstance> inputTypes, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
    }

    public TPreptimeContext(List<TInstance> inputTypes, TInstance outputType, QueryContext queryContext, SparseArray<Object> preptimeCache) {
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
        this.preptimeCache = preptimeCache;
    }

    private final QueryContext queryContext;
    private SparseArray<Object> preptimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
}
