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
package com.foundationdb.rest;

import com.foundationdb.rest.dml.RestDMLService;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;

public class ResourceRequirements {
    public final DXLService dxlService;
    public final RestDMLService restDMLService;
    public final SecurityService securityService;
    public final SessionService sessionService;
    public final TransactionService transactionService;
    public final Store store;
    public final ConfigurationService configService;

    public ResourceRequirements(DXLService dxlService,
                                RestDMLService restDMLService,
                                SecurityService securityService,
                                SessionService sessionService,
                                TransactionService transactionService,
                                Store store,
                                ConfigurationService configService) {
        this.dxlService = dxlService;
        this.restDMLService = restDMLService;
        this.securityService = securityService;
        this.sessionService = sessionService;
        this.transactionService = transactionService;
        this.store = store;
        this.configService = configService;
    }
}
