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
package com.foundationdb.server.service.dxl;

import com.foundationdb.server.service.session.Session;

public interface DXLFunctionsHook {

    static enum DXLType {
        DDL_FUNCTIONS_WRITE,
        DDL_FUNCTIONS_READ,
        DML_FUNCTIONS_WRITE,
        DML_FUNCTIONS_READ
    }

    static enum DXLFunction {
        CREATE_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        RENAME_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        ALTER_TABLE(DXLType.DDL_FUNCTIONS_WRITE),
        ALTER_SEQUENCE(DXLType.DDL_FUNCTIONS_WRITE),
        CREATE_VIEW(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_VIEW(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_SCHEMA(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_GROUP(DXLType.DDL_FUNCTIONS_WRITE),
        GET_AIS(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_TABLE_BY_NAME(DXLType.DDL_FUNCTIONS_READ),
        GET_USER_TABLE_BY_ID(DXLType.DDL_FUNCTIONS_READ),
        GET_ACTIVE_GENERATIONS(DXLType.DDL_FUNCTIONS_READ),
        CREATE_INDEXES(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_INDEXES(DXLType.DDL_FUNCTIONS_WRITE),
        CREATE_SEQUENCE(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_SEQUENCE(DXLType.DDL_FUNCTIONS_WRITE),
        CREATE_ROUTINE(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_ROUTINE(DXLType.DDL_FUNCTIONS_WRITE),
        CREATE_SQLJ_JAR(DXLType.DDL_FUNCTIONS_WRITE),
        REPLACE_SQLJ_JAR(DXLType.DDL_FUNCTIONS_WRITE),
        DROP_SQLJ_JAR(DXLType.DDL_FUNCTIONS_WRITE),
        
        TRUNCATE_TABLE(DXLType.DML_FUNCTIONS_WRITE),
        UPDATE_TABLE_STATISTICS(DXLType.DML_FUNCTIONS_WRITE),

        
        // For use by Postgres
        UNSPECIFIED_DDL_WRITE(DXLType.DDL_FUNCTIONS_WRITE),
        UNSPECIFIED_DDL_READ(DXLType.DDL_FUNCTIONS_READ),
        UNSPECIFIED_DML_WRITE(DXLType.DML_FUNCTIONS_WRITE),
        UNSPECIFIED_DML_READ(DXLType.DML_FUNCTIONS_READ),
        ;

        private final DXLType type;

        DXLFunction(DXLType type) {
            this.type = type;
        }

        public DXLType getType() {
            return type;
        }
    }
    void hookFunctionIn(Session session, DXLFunction function);
    void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable);
    void hookFunctionFinally(Session session, DXLFunction function, Throwable throwable);
}
