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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.server.error.SequenceIntervalZeroException;
import com.foundationdb.server.error.SequenceMinGEMaxException;
import com.foundationdb.server.error.SequenceStartInRangeException;

public class SequenceValuesValid implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Sequence sequence : ais.getSequences().values()) {
            if (sequence.getIncrement() == 0) {
                output.reportFailure(new AISValidationFailure (
                        new SequenceIntervalZeroException()));
            }
            if (sequence.getMinValue() >= sequence.getMaxValue()) {
                output.reportFailure(new AISValidationFailure (
                        new SequenceMinGEMaxException()));
            }
            if (sequence.getStartsWith() < sequence.getMinValue() ||
                    sequence.getStartsWith() > sequence.getMaxValue()) {
                output.reportFailure(new AISValidationFailure(
                        new SequenceStartInRangeException ()));
            }
                
                
        }
    }
}
