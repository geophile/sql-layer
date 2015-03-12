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
package com.foundationdb.server.types.aksql;

import com.foundationdb.server.error.LobUnsupportedException;
import com.foundationdb.server.error.InvalidGuidFormatException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.UUID;

public class AkParsers
{
    public static final TParser BOOLEAN = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            String s = source.getString();
            boolean result = false;
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t")) {
                result = true;
            }
            else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f")) {
                result = false;
            }
            else {
                // parse source is a string representing a number-ish, where '0' is false, any other integer is true.
                // We're looking for an optional negative, followed by an optional dot, followed by any number of digits,
                // followed by anything. If any of those digits is not 0, the result is true; otherwise it's false.
                boolean negativeAllowed = true;
                boolean periodAllowed = true;
                for (int i = 0, len = s.length(); i < len; ++i) {
                    char c = s.charAt(i);
                    if (negativeAllowed && c == '-') {
                        negativeAllowed = false;
                    }
                    else if (periodAllowed && c == '.') {
                        periodAllowed = false;
                        negativeAllowed = false;
                    }
                    else if (Character.isDigit(c)) {
                        if (c != '0') {
                            result = true;
                            break;
                        }
                    }
                    else {
                        break;
                    }
                }
            }
            target.putBool(result);
        }
    };
    
    public static final TParser GUID = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            String s = source.getString();
            if (s.startsWith("{") && s.endsWith("}")) {
                s = s.substring(1, s.length()-1);
            }
            try {
                UUID uuid = UUID.fromString(s);
                target.putObject(uuid);
            } catch (IllegalArgumentException e) {
                throw new InvalidGuidFormatException(s);
            }
        }
    };

    public static final TParser BLOB = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            throw new LobUnsupportedException("String parsing unsupported");
        }
    };
}
