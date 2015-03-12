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

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Full text query against index.
 */
@Path("/text/{table}/{index}")
public class FullTextResource {
    private final ResourceRequirements reqs;

    public FullTextResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response textSearch(@Context final HttpServletRequest request,
                               @PathParam("table") String table,
                               @PathParam("index") String index,
                               @QueryParam("q") final String query,
                               @QueryParam("depth") final Integer depth,
                               @QueryParam("size") final Integer limit) throws Exception {
        final IndexName indexName = new IndexName(ResourceHelper.parseTableName(request, table), index);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, indexName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.fullTextSearch(writer, indexName, depth, query, limit);
                    }
                })
                .build();
    }
}
