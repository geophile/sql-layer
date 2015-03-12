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
package com.foundationdb.server.collation;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ibm.icu.text.Collator;
import com.persistit.Key;
import com.persistit.util.Util;

import java.util.Arrays;

public class AkCollatorICU extends AkCollator {

    private final CollationSpecifier collationSpecifier;

    final ThreadLocal<Collator> collator = new ThreadLocal<Collator>() {
        protected Collator initialValue() {
            return AkCollatorFactory.forScheme(collationSpecifier);
        }
    };

    /**
     * Create an AkCollator which may be used in across multiple threads. Each
     * instance of AkCollator has a ThreadLocal which optionally contains a
     * reference to a thread-private ICU4J Collator.
     * 
     * @param name
     *            Name given to AkCollatorFactory by which to look up the scheme
     * @param scheme
     *            Formatted string containing Locale name, and collation string
     *            strength.
     */
    AkCollatorICU(final String scheme, final int collationId) {
        super(scheme, collationId);
        collationSpecifier = new CollationSpecifier(scheme);
        collator.get(); // force the collator to initialize (to test scheme)
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void append(Key key, String value) {
        if (value == null) {
            key.append(null);
        } else {
            key.append(encodeSortKeyBytes(value));
        }
    }

    @Override
    public int compare(String source, String target) {
        return collator.get().compare(source, target);
    }

    @Override
    public boolean isCaseSensitive() {
        return collator.get().getStrength() > Collator.SECONDARY;
    }

    /**
     * Construct the sort key bytes for the given String value
     * 
     * @param value
     *            the String
     * @return sort key bytes
     */
    @Override
    public byte[] encodeSortKeyBytes(String value) {
        byte[] bytes = collator.get().getCollationKey(value).toByteArray();
        return Arrays.copyOf(bytes, bytes.length - 1); // Remove terminating null.
    }

    /** Decode the value to a string of hex digits. */
    @Override
    String debugDecodeSortKeyBytes(byte[] bytes, int index, int length) {
        StringBuilder sb = new StringBuilder();
        Util.bytesToHex(sb, bytes, index, length);
        return sb.toString();
    }

    @Override
    public int hashCode(String string) {
        byte[] bytes = collator.get().getCollationKey(string).toByteArray();
        bytes = Arrays.copyOfRange(bytes, 0, bytes.length-1); // remove null terminating character
        return hashCode(bytes);
    }

    @Override
    public int hashCode(byte[] bytes) {
        return hashFunction.hashBytes(bytes, 0, bytes.length).asInt();
    }

    @Override
    public String toString() {
        return collationSpecifier.toString(); 
    }

    @Override
    public String getScheme() {
        return collationSpecifier.toString();
    }

    private static final HashFunction hashFunction = Hashing.goodFastHash(32); // Because we're returning ints
}
