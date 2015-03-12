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

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

/**
 * The categories of error code classes that are part of the public API.
 *
 * From the SQL Standard, the SQLSTATE (ErrorCodes) are a 2 character class value
 * followed by a 3 character sub-class value. These characters are either digits or
 * upper-case latin characters. (0-9 or A-Z).
**/
public class ErrorCodeClass {

    static final ResourceBundle resourceBundle = ResourceBundle.getBundle("com.foundationdb.server.error.error_code_class");
    private final String key;
    private final String description;

    private ErrorCodeClass(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }

    public static List<ErrorCodeClass> getClasses() {
        List<ErrorCodeClass> classes = new ArrayList<>(60);
        for (Enumeration<String> keys = resourceBundle.getKeys(); keys.hasMoreElements();) {
            String key = keys.nextElement();
            classes.add(new ErrorCodeClass(key, resourceBundle.getString(key)));
        }
        return classes;
    }

}
