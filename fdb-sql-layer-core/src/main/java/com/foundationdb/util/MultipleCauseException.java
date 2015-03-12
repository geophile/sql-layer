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
package com.foundationdb.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MultipleCauseException extends RuntimeException {
    private final List<Throwable> causes = new ArrayList<>();

    public void addCause(Throwable cause) {
        causes.add(cause);
    }

    @Override
    public String toString() {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf("%d cause%s%n", causes.size(), causes.size() == 1 ? ":" : "s:");
        for (Enumerated<Throwable> cause : EnumeratingIterator.of(causes)) {
            printWriter.printf("%d:%n----------------------------%n", cause.count());
            cause.get().printStackTrace(printWriter);
        }
        printWriter.flush();
        stringWriter.flush();
        return stringWriter.toString();
    }

    public List<Throwable> getCauses() {
        return Collections.unmodifiableList(causes);
    }
    
    public static RuntimeException combine(RuntimeException oldProblem, RuntimeException newProblem) {
        if (oldProblem == null)
            return newProblem;
        if (oldProblem instanceof MultipleCauseException) {
            MultipleCauseException mce = (MultipleCauseException) oldProblem;
            mce.addCause(newProblem);
            return mce;
        }
        MultipleCauseException mce = new MultipleCauseException();
        mce.addCause(oldProblem);
        mce.addCause(newProblem);
        return mce;
    }
}
