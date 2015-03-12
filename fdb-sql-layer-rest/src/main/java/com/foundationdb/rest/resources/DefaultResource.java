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
package com.foundationdb.rest.resources;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.error.ErrorCode;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

@Path("{other:.*}")
public class DefaultResource {

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleGetNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @PUT
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePutNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @POST
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePostNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @DELETE
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleDeleteNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }

    @PATCH
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePatchNoResource(@Context HttpServletRequest request) {
        return buildResponse (request);
    }
    
    
    static Response buildResponse(HttpServletRequest request) {
        String msg = String.format("API %s not supported", request.getRequestURI());
        return RestResponseBuilder
                .forRequest(request)
                .status(Response.Status.NOT_FOUND)
                .body(ErrorCode.MALFORMED_REQUEST, msg)
                .build();
    }
    
    
}
