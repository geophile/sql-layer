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
package com.foundationdb.ais.model.aisb2;

public interface NewAkibanJoinBuilder extends NewTableBuilder {
    /**
     * Adds a child -&gt; parent column pair to this join.
     *
     * <p>{@linkplain #and(String, String)} is a synonym of this method.</p>
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder on(String childColumn, String parentColumn);

    /**
     * Synonym for {@link #on(String, String)}. This method is just here to make the code more "English-sounding."
     * Example: {@code build.joinTo("parent").on("child_col_1", "parent_col_1").and("child_col_2", "parent_col_2").}
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder and(String childColumn, String parentColumn);
}
