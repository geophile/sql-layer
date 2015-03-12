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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.UnsupportedCharsetException;

import java.util.HashMap;
import java.util.Map;

public class StringFactory
{
    //--------------------------------CHARSET-----------------------------------
    //TODO: add more charsets as needed
    public static enum Charset
    {
        LATIN1, UTF8, UTF16, ISO_8859_1
        ;
        
        public static Charset of(String value) {
            // Could optimize this with a StringBuilder, for-loop, etc
            value = value.toUpperCase();
            Charset charset = lookupMap.get(value);
            if (charset == null)
                throw new UnsupportedCharsetException(value);
            return charset;
        }
        
        public static String of (int ordinal)
        {
            return Charset.values()[ordinal].name();
        }

        private static final Map<String,Charset> lookupMap = createLookupMap();

        private static Map<String, Charset> createLookupMap() {
            Map<String,Charset> map = new HashMap<>();
            for (Charset charset : Charset.values()) {
                map.put(charset.name(), charset);
            }
            // aliases
            map.put("ISO-8859-1", LATIN1);
            map.put("UTF-8", UTF8);
            map.put("UTF-16", UTF16);
            return map;
        }
    }
    
    public static int charsetNameToId(String name) {
        return (name == null) ? NULL_CHARSET_ID : Charset.of(name).ordinal();
    }

    public static String charsetIdToName(int id) {
        return (id == NULL_CHARSET_ID) ? null : Charset.of(id);
    }

    public static int collationNameToId(String name) {
        return (name == null) ? NULL_COLLATION_ID : AkCollatorFactory.getAkCollator(name).getCollationId();
    }

    public static String collationIdToName(int id) {
        return (id == NULL_COLLATION_ID) ? null : AkCollatorFactory.getAkCollator(id).getScheme();
    }

    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    protected static final int DEFAULT_LENGTH = 255;
    
    public static final Charset DEFAULT_CHARSET = Charset.UTF8;
    public static final int DEFAULT_CHARSET_ID = DEFAULT_CHARSET.ordinal();
    public static final int NULL_CHARSET_ID = -1;
    
    public static final int DEFAULT_COLLATION_ID = AkCollatorFactory.UCS_BINARY_ID;
    public static final int NULL_COLLATION_ID = -1; // String literals
    
    //--------------------------------------------------------------------------

    private StringFactory() {}
}
