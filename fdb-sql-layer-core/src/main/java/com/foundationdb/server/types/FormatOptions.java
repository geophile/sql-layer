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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.util.Strings;

import java.util.HashMap;
import java.util.Map;


public class FormatOptions {

    public interface FormatOption {
        public String format(byte[] bytes);
    }
    
    public static enum BinaryFormatOption implements FormatOption {
        OCTAL,
        HEX,
        BASE64;

        public static BinaryFormatOption fromProperty(String name) {
            try {
                return BinaryFormatOption.valueOf(name.toUpperCase());
            }
            catch(IllegalArgumentException iae) {
                throw new InvalidParameterValueException(name);
            }
        }

        public String format(byte[] bytes) {
            StringBuilder out = new StringBuilder();
            if (this.equals(FormatOptions.BinaryFormatOption.OCTAL)) {
                out.append(Strings.toOctal(bytes));
            } else if (this.equals(FormatOptions.BinaryFormatOption.HEX)) {
                out.append("\\x");
                out.append(Strings.hex(bytes));
            } else if (this.equals(FormatOptions.BinaryFormatOption.BASE64)) {
                out.append(Strings.toBase64(bytes));
            }
            return out.toString();
        }
    }

    public static enum JsonBinaryFormatOption implements FormatOption {
        OCTAL,
        HEX,
        BASE64;

        public static JsonBinaryFormatOption fromProperty(String name) {
            try {
                return JsonBinaryFormatOption.valueOf(name.toUpperCase());
            }
            catch(IllegalArgumentException iae) {
                throw new InvalidParameterValueException(name);
            }
        }
        
        public String format(byte[] bytes) {
            StringBuilder out = new StringBuilder();
            if (this.equals(FormatOptions.JsonBinaryFormatOption.OCTAL)) {
                out.append(Strings.toOctal(bytes));
            } else if (this.equals(FormatOptions.JsonBinaryFormatOption.HEX)) {
                out.append("\\x");
                out.append(Strings.hex(bytes));
            } else if (this.equals(FormatOptions.JsonBinaryFormatOption.BASE64)) {
                out.append(Strings.toBase64(bytes));
            }
            return out.toString();
        }
    }

    private final Map<Class<? extends FormatOption>, FormatOption> opts = new HashMap<>();

    public <T extends FormatOption> void set(T value) {
        opts.put(value.getClass(), value);
    }

    @SuppressWarnings("unchecked")
    public <T extends FormatOption> T get(Class<T> clazz) {
        return (T)opts.get(clazz);
    }
    
}
