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
package com.foundationdb.http;

import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.RoleInfo;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.servlets.CrossOriginFilter;

/**
 * <p>
 *     An subclass of {@link ConstraintSecurityHandler} that allows a CORS preflight through without authentication.
 * </p>
 * <p>
 *     Preflight processing is required for an non-simple request, which includes <i>any</i> request that requires
 *     authentication. It also <b>cannot</b> be authentication itself. This handler allows just the preflight OPTIONS
 *     request to come through un-authenticated, which allows the proper {@link CrossOriginFilter} handling to kick in.
 * </p>
 * <p>
 *     See the <a href="http://www.w3.org/TR/cors/#simple-header">CORS spec</a> for reference, particularly the
 *     <a href="http://www.w3.org/TR/cors/#resource-preflight-requests">Preflight</a> and
 *     <a href="http://www.w3.org/TR/cors/#simple-header">Simple Header</a> sections.
 * </p>
 */
public class CrossOriginConstraintSecurityHandler extends ConstraintSecurityHandler {
    private static final String ORIGIN_HEADER = "Origin";

    @Override
    protected boolean isAuthMandatory(Request baseRequest, Response baseResponse, Object constraintInfo) {
        if(constraintInfo == null) {
            return false;
        }
        if(isPreFlightRequest(baseRequest)) {
            return false;
        }
        return ((RoleInfo)constraintInfo).isChecked();
    }

    private static boolean isPreFlightRequest(Request request) {
        if(HttpMethods.OPTIONS.equalsIgnoreCase(request.getMethod())) {
            // If the origin does not match allowed the filter will skip anyway so don't bother checking it.
            if(request.getHeader(ORIGIN_HEADER) != null &&
               request.getHeader(CrossOriginFilter.ACCESS_CONTROL_REQUEST_METHOD_HEADER) != null) {
                return true;
            }
        }
        return false;
    }
}
