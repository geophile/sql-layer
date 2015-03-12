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

import com.foundationdb.util.ArgumentValidation;

import java.util.regex.Pattern;

public final class TName {

    public static String normalizeName(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        String[] words = name.split("\\s+");
        for (int i = 0, wordsLength = words.length; i < wordsLength; i++) {
            String word = words[i];
            if (!WORD_VALIDATION.matcher(word).matches())
                throw new IllegalNameException("illegal type name: " + name);
            sb.append(word.toUpperCase());
            if (i+1 < wordsLength)
                sb.append(' ');
        }
        return sb.toString();
    }

    public String unqualifiedName() {
        return name;
    }

    public TBundleID bundleId() {
        return bundleID;
    }
    
    public String categoryName() {
        return (category == null) ? "OTHER" : category.name();
    }

    // object interface

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TName other = (TName) o;
        return bundleID.equals(other.bundleID) && name.equals(other.name);
    }

    @Override
    public int hashCode() {
        int result = bundleID.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return bundleID + "_ " + name;
    }

    public TName(TBundleID bundleID, String name, Enum<?> category) {
        ArgumentValidation.notNull("bundle", bundleID);
        ArgumentValidation.notNull("name", name);
        this.bundleID = bundleID;
        this.name = normalizeName(name);
        this.category = category;
    }

    private final TBundleID bundleID;
    private final String name;
    private final Enum<?> category;
    private static final Pattern WORD_VALIDATION = Pattern.compile("[a-zA-Z][a-zA-Z0-9]*");
}
