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
import com.foundationdb.qp.operator.QueryContext.NotificationLevel;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.OverflowException;
import com.foundationdb.server.error.StringTruncationException;
import com.foundationdb.util.SparseArray;
import com.google.common.base.Objects;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

public final class TExecutionContext {

    public Object objectAt(int index) {
        Object result = null;
        if (preptimeCache != null && preptimeCache.isDefined(index))
            result = preptimeCache.get(index);
        if (result == null && exectimeCache != null && exectimeCache.isDefined(index))
            result = exectimeCache.get(index);
        return result;
    }

    public TInstance outputType() {
        return outputType;
    }

    public Object preptimeObjectAt(int index) {
        if ((preptimeCache != null) && preptimeCache.isDefined(index))
            return preptimeCache.get(index);
        else
            return null;
    }

    public boolean hasExectimeObject(int index) {
        return exectimeCache != null && exectimeCache.isDefined(index);
    }

    public Object exectimeObjectAt(int index) {
        if (exectimeCache == null)
            exectimeCache = new SparseArray<>();
        return exectimeCache.get(index);
    }

    public void putExectimeObject(int index, Object value) {
        if (preptimeCache != null && preptimeCache.isDefined(index)) {
            Object conflict = preptimeCache.get(index);
            if (conflict != null)
                throw new IllegalStateException("conflicts with preptime value: " + conflict);
        }
        if (exectimeCache == null)
            exectimeCache = new SparseArray<>(index);
        exectimeCache.set(index, value);
    }

    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        if (queryContext == null)
            logger.warn("no query context on which to report error {}: {}", errorCode, message);
        else
            queryContext.notifyClient(level, errorCode, message);
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }
    
    public void warnClient(InvalidOperationException exception) {
        if (queryContext == null)
            logger.warn("no query context on which to report exception", exception);
        else
            queryContext.warnClient(exception);
    }

    public void logError (String msg)
    {
        logger.error(msg);
    }
    
    public Locale getCurrentLocale()
    {
        // TODO: need to get this from the session
        return Locale.getDefault();
    }

    /**
     * Some functions need to get the current timezone (session/global), not the JVM's timezone.
     * @return  the server's timezone.
     */
    public String getCurrentTimezone()
    {
        // TODO need to get this from the session
        return DateTimeZone.getDefault().getID();
    }

    /**
     * 
     * @return  the time at which the query started
     */
    public long getCurrentDate()
    {
        return queryContext.getStartTime();
    }
    
    public String getCurrentUser()
    {
        return queryContext.getCurrentUser();
    }
    
    public String getSessionUser()
    {
        return queryContext.getSessionUser();
    }
    
    public String getSystemUser()
    {
        return queryContext.getSystemUser();
    }
    
    public String getCurrentSchema()
    {
        return queryContext.getCurrentSchema();
    }
    
    public String getCurrentSetting(String key)
    {
        return queryContext.getCurrentSetting(key);
    }

    public int getSessionId()
    {
        return queryContext.getSessionId();
    }
    
    public void reportOverflow(String msg)
    {
        switch(overflowHandling)
        {
            case WARN:
                warnClient(new OverflowException());
                break;
            case ERROR:
                throw new OverflowException();
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(overflowHandling);
        }
    }
    
    public void reportTruncate(String original, String truncated)
    {
        switch(truncateHandling)
        {
            case WARN:
                warnClient(new StringTruncationException(original, truncated));
                break;
            case ERROR:
                throw new StringTruncationException(original, truncated);
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(truncateHandling);
        }
    }
    
    public void reportBadValue(String msg)
    {
        switch(invalidFormatHandling)
        {
            case WARN:
                warnClient(new InvalidParameterValueException(msg));
                break;
            case ERROR:
                throw new InvalidParameterValueException(msg);
            case IGNORE:
                // ignores, does nothing
                break;
            default:
                throw new AssertionError(invalidFormatHandling);
        }
    }

    public void setQueryContext(QueryContext queryContext) {
        this.queryContext = queryContext;
    }

    public TExecutionContext deriveContext(List<TInstance> inputTypes, TInstance outputType) {
        return new TExecutionContext(
                new SparseArray<>(),
                inputTypes,
                outputType,
                queryContext,
                overflowHandling,
                truncateHandling,
                invalidFormatHandling
        );
    }

    // state

    public TExecutionContext(List<TInstance> inputTypes,  TInstance outputType, QueryContext queryContext) {
        this(null, inputTypes, outputType, queryContext, null, null, null);
    }

    public TExecutionContext(SparseArray<Object> preptimeCache,
                      List<TInstance> inputTypes,
                      TInstance outputType,
                      QueryContext queryContext,
                      ErrorHandlingMode overflow,
                      ErrorHandlingMode truncate,
                      ErrorHandlingMode invalid)
    {
        this.preptimeCache = preptimeCache;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.queryContext = queryContext;
        overflowHandling = Objects.firstNonNull(overflow, ErrorHandlingMode.WARN);
        truncateHandling = Objects.firstNonNull(truncate, ErrorHandlingMode.WARN);
        invalidFormatHandling = Objects.firstNonNull(invalid,  ErrorHandlingMode.WARN);
    }

    private SparseArray<Object> preptimeCache;
    private SparseArray<Object> exectimeCache;
    private List<TInstance> inputTypes;
    private TInstance outputType;
    private QueryContext queryContext;
    private ErrorHandlingMode overflowHandling;
    private ErrorHandlingMode truncateHandling;
    private ErrorHandlingMode invalidFormatHandling;

    private static final Logger logger = LoggerFactory.getLogger(TExecutionContext.class);
}
