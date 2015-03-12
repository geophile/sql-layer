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

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;

import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.error.ErrorCode;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ErrorHandler;

public class JsonErrorHandler extends ErrorHandler {

    public static final String PATCH_METHOD = "PATCH";

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
        baseRequest.setHandled(true);
        String method = request.getMethod();
        if(!method.equals(HttpMethods.HEAD) && !method.equals(HttpMethods.GET) &&
                !method.equals(HttpMethods.POST) && !method.equals(PATCH_METHOD) && !method.equals(HttpMethods.PUT) &&
                 !method.equals(HttpMethods.DELETE)) {
            return;
        }

        final String message;
        final ErrorCode error;
        final String note;
        if(response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            message = "Path not found";
            if (!request.getRequestURI().contains("/v1/")) {
                note = "try including /v1/ in the path";
            } else {
                note = null;
            }
            error = ErrorCode.MALFORMED_REQUEST;
        } else {
            if (response instanceof Response) {
                note = ((Response)response).getReason();
            } else {
                note = null;
            }
            message = HttpStatus.getMessage(response.getStatus());
            error = ErrorCode.INTERNAL_ERROR;
        }

        response.setContentType(MediaType.APPLICATION_JSON);
        response.setHeader(HttpHeaders.CACHE_CONTROL, getCacheControl());

        StringBuilder builder = new StringBuilder();
        RestResponseBuilder.formatJsonError(builder, error.getFormattedValue(), message, note);
        builder.append('\n');

        response.setContentLength(builder.length());
        OutputStream out = response.getOutputStream();
        out.write(builder.toString().getBytes());
        out.close();
    }
}
