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
package com.foundationdb.server;

import com.foundationdb.sql.Main;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class GetVersion
{
    public static void main(String[] args) throws Exception {
        if(args.length > 0 && "-v".equals(args[0])) {
            dumpVerbose();
        } else {
            dumpMinimal();
        }
    }

    private static void dumpVerbose() throws IllegalAccessException {
        for(Field field : Main.VERSION_INFO.getClass().getDeclaredFields()) {
            if((field.getModifiers() & Modifier.PUBLIC) > 0) {
                System.out.println(field.getName() + "=" + field.get(Main.VERSION_INFO));
            }
        }
    }

    private static void dumpMinimal() {
        System.out.println(Main.VERSION_INFO.versionLong);
    }
}
