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
package com.foundationdb.ais.model.validation;

import java.util.Collection;
import java.util.Collections;

import com.foundationdb.util.MultipleCauseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AISValidationResults {
    private static final Logger LOG = LoggerFactory.getLogger(AISValidationResults.class);

    /**
     * Gets all failures, if there were any.
     *
     * The collection will be unmodifiable and immutable; if it is not empty, subsequent invocations of this method
     * may all return same collection instance, but only as long as no additional failures are reported. If new
     * failures are reported, they and the previous will be in a new collection instance.
     * @return an unmodifiable, immutable collection of failures; will be empty if all validations passed.
     */
    public Collection<AISValidationFailure> failures() {
        return Collections.unmodifiableCollection(failureList);
    }
    /**
     * Throws an InvalidOperationException if failures() returns a non-null collection.
     */
    public void throwIfNecessary() {
        if (!failureList.isEmpty()) {
            RuntimeException ex = null;
            for(AISValidationFailure f : failureList) {
                LOG.debug(String.format("Validation failure %s : %s", f.errorCode(), f.message()));
                ex = MultipleCauseException.combine(ex, f.getException());
            }
            throw ex;
        }
    }
 
    /**
     * Package-private constructor
     */
    protected  AISValidationResults() {}
    protected Collection<AISValidationFailure> failureList;
}
