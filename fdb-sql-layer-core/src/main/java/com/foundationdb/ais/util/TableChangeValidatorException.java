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
package com.foundationdb.ais.util;

import static java.lang.String.format;

public class TableChangeValidatorException extends IllegalArgumentException {
    private static final String COLUMN = "column";
    private static final String INDEX = "index";
    private static final String ADD_NOT_PRESENT_MSG = "ADD %s not in new table: %s";
    private static final String DROP_NOT_PRESENT_MSG = "DROP %s not in old table: %s";
    private static final String MODIFY_NOT_PRESENT_MSG = "MODIFY %s not in old or new table: %s";
    private static final String MODIFY_NOT_CHANGED_MSG = "MODIFY %s not changed: %s";
    private static final String UNCHANGED_NOT_PRESENT_MSG = "Unchanged %s not present in new table: %s";
    private static final String UNDECLARED_CHANGE_MSG = "Undeclared %s change in new table: %s";

    public TableChangeValidatorException(String detail) {
        super(detail);
    }

    //
    // Column
    //

    public static class AddColumnNotPresentException extends TableChangeValidatorException {
        public AddColumnNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class DropColumnNotPresentException extends TableChangeValidatorException {
        public DropColumnNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotPresentException extends TableChangeValidatorException {
        public ModifyColumnNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class ModifyColumnNotChangedException extends TableChangeValidatorException {
        public ModifyColumnNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, COLUMN, detail));
        }
    }

    public static class UnchangedColumnNotPresentException extends TableChangeValidatorException {
        public UnchangedColumnNotPresentException(String detail) {
            super(format(UNCHANGED_NOT_PRESENT_MSG, COLUMN, detail));
        }
    }

    public static class UndeclaredColumnChangeException extends TableChangeValidatorException {
        public UndeclaredColumnChangeException(String detail) {
            super(format(UNDECLARED_CHANGE_MSG, COLUMN, detail));
        }
    }

    //
    // Index
    //

    public static class AddIndexNotPresentException extends TableChangeValidatorException {
        public AddIndexNotPresentException(String detail) {
            super(format(ADD_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class DropIndexNotPresentException extends TableChangeValidatorException {
        public DropIndexNotPresentException(String detail) {
            super(format(DROP_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotPresentException extends TableChangeValidatorException {
        public ModifyIndexNotPresentException(String detail) {
            super(format(MODIFY_NOT_PRESENT_MSG, INDEX, detail));
        }
    }

    public static class ModifyIndexNotChangedException extends TableChangeValidatorException {
        public ModifyIndexNotChangedException(String detail) {
            super(format(MODIFY_NOT_CHANGED_MSG, INDEX, detail));
        }
    }
}
