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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.PrintWriter;

import static com.foundationdb.rest.resources.ResourceHelper.JSONP_ARG_NAME;
import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Allows calling stored procedures directly.
 */
@Path("/call/{proc}")
public class ProcedureCallResource {
    private final ResourceRequirements reqs;
    
    public ProcedureCallResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @POST
    @Consumes(MEDIATYPE_JSON_JAVASCRIPT)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response postCall(@Context final HttpServletRequest request,
                             @PathParam("proc") String proc,
                             @Context final UriInfo uri,
                             final String jsonParams) throws Exception {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.callProcedure(writer, request, JSONP_ARG_NAME,
                                                          procName, uri.getQueryParameters(), jsonParams);
                    }
                })
                .build();
    }
}
