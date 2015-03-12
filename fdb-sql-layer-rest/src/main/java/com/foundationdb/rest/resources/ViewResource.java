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

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.foundationdb.rest.resources.ResourceHelper.checkTableAccessible;
import static com.foundationdb.rest.resources.ResourceHelper.parseTableName;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.error.WrongExpressionArityException;

@Path("/view/{view}")

public class ViewResource {
    private final ResourceRequirements reqs;
    
    public ViewResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context final HttpServletRequest request,
                                   @PathParam("view") String view,
                                   @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, view);
        checkTableAccessible(reqs.securityService, request, tableName);
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ");
        query.append(tableName.toString());
        List<String> params = new ArrayList<>();
        boolean first = true;
        
        for (Map.Entry<String,List<String>> entry : uri.getQueryParameters().entrySet()) {
            if (entry.getValue().size() != 1)
                throw new WrongExpressionArityException(1, entry.getValue().size());
            if (ResourceHelper.JSONP_ARG_NAME.equals(entry.getKey()))
                continue;
            if (first) {
                query.append(" WHERE ");
                first = false;
            } else {
                query.append(" AND ");
            }
            query.append (entry.getKey());
            query.append("=?");
            params.add(entry.getValue().get(0));
        }

        final String queryFinal = query.toString();
        final List<String> parameters = Collections.unmodifiableList(params);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQLParameter(writer, request, queryFinal, parameters);
                    }
                })
                .build();
    }
}
