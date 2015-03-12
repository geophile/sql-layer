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
package com.foundationdb.server.error;

import com.foundationdb.server.types.TInputSet;

public class ArgumentTypeRequiredException extends InvalidOperationException
{
    public ArgumentTypeRequiredException(String functionName, int argPosition) {
        this(functionName, Integer.toString(argPosition));
    }

    public ArgumentTypeRequiredException(String functionName, TInputSet inputSet) {
        this(functionName, coveringDesc(inputSet));
    }

    private ArgumentTypeRequiredException(String functionName, String inputDesc) {
        super(ErrorCode.ARGUMENT_TYPE_REQUIRED, functionName, inputDesc);
    }

    private static String coveringDesc(TInputSet inputSet) {
        StringBuilder sb = new StringBuilder();
        for(int i = inputSet.firstPosition(); i >= 0; i = inputSet.nextPosition(i+1)) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append(i);
        }
        return sb.toString();
    }
}
