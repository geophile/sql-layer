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
package com.foundationdb.ais.model;

import java.util.Arrays;

/**
 * IndexRowComposition presents an interface for mapping row and hkey fields
 * to the fields of an index. The leading index fields are exactly the fields
 * identified in the Index (i.e. the declared index columns). The remaining
 * fields are whatever is necessary to ensure that all of the hkey is represented.
 */
public class IndexRowComposition {
    public IndexRowComposition(int[] fieldPositions, int[] hkeyPositions) {
        if(fieldPositions.length != hkeyPositions.length) {
            throw new IllegalArgumentException("Both arrays must be of equal length: " +
                                               fieldPositions.length + ", " +
                                               hkeyPositions.length);
        }
        this.fieldPositions = fieldPositions;
        this.hkeyPositions = hkeyPositions;
    }

    public boolean isInRow(int indexPos) {
        return fieldPositions[indexPos] >= 0;
    }

    public boolean isInHKey(int indexPos) {
        return hkeyPositions[indexPos] >= 0;
    }

    public int getFieldPosition(int indexPos) {
        return fieldPositions[indexPos];
    }

    public int getHKeyPosition(int indexPos) {
        return hkeyPositions[indexPos];
    }

    public int getLength() {
        return fieldPositions.length;
    }

    @Override
    public String toString() {
        return "fieldPos: " + Arrays.toString(fieldPositions) +
               " hkeyPos: " + Arrays.toString(hkeyPositions);
    }

    /** If set, value >= 0, is the field position for index position i **/
    private final int[] fieldPositions;
    /** If set, value >= 0, is the hkey position for index position i **/
    private final int[] hkeyPositions;
}
