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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

@Path("/sql")
public class SQLResource {
    private static final Logger logger = LoggerFactory.getLogger(SQLResource.class);

    private final ResourceRequirements reqs;

    public SQLResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    /** Run a single SQL statement specified by the 'q' query parameter. */
    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response query(@Context final HttpServletRequest request,
                          final String jsonParams) {
        logger.debug("/sql/query: {}", jsonParams);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        Map<String, String> paramMap = new ObjectMapper().readValue(jsonParams, 
                                                                                    new TypeReference<Map<String, String>>() {});
                        reqs.restDMLService.runSQL(writer, request, paramMap.get("q"), null);
                    }
                })
                .build();
    }

    /** Explain a single SQL statement specified by the 'q' query parameter. */
    @POST
    @Path("/explain")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response explain(@Context final HttpServletRequest request,
                            final String jsonParams) {
        logger.debug("/sql/explain: {}", jsonParams);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        Map<String, String> paramMap = new ObjectMapper().readValue(jsonParams, 
                                                                                    new TypeReference<Map<String, String>>() {});
                        reqs.restDMLService.explainSQL(writer, request, paramMap.get("q"));
                    }
                })
                .build();
    }

    /** Run multiple SQL statements (single transaction) specified by semi-colon separated strings in the POST body. */
    @POST
    @Path("/execute")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response execute(@Context final HttpServletRequest request,
                            final byte[] postBytes) {
        String input = new String(postBytes);
        logger.debug("/sql/execute: {}", input);
        final String[] statements = input.split(";");
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, Arrays.asList(statements));
                    }
                })
                .build();
    }
}
