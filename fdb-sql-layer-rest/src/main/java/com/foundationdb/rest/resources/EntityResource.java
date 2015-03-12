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
import com.fasterxml.jackson.databind.JsonNode;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.PrintWriter;

import static com.foundationdb.rest.resources.ResourceHelper.IDENTIFIERS_MULTI;
import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.foundationdb.rest.resources.ResourceHelper.checkTableAccessible;
import static com.foundationdb.rest.resources.ResourceHelper.getPKString;
import static com.foundationdb.rest.resources.ResourceHelper.parseTableName;
import static com.foundationdb.util.JsonUtils.readTree;

/**
 * Entity based access (GET), creation (PUT, POST), and modification (PUT, DELETE)
 */
@Path("/entity/{entity}")
public class EntityResource {
    private final ResourceRequirements reqs;
    
    public EntityResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.getAllEntities(writer, tableName, depth);
                    }
                })
                .build();
    }

    @GET
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context HttpServletRequest request,
                                   @PathParam("entity") String entity,
                                   @QueryParam("depth") final Integer depth,
                                   @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.getEntities(writer, tableName, depth, getPKString(uri));
                    }
                })
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response createEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        final JsonNode node = readTree(entityBytes);
                        reqs.restDMLService.insert(writer, tableName, node);
                    }
                })
                .build();
    }

    @PUT
    @Path("/" + IDENTIFIERS_MULTI)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response updateEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 final byte[] entityBytes,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        JsonNode node = readTree(entityBytes);
                        reqs.restDMLService.update(writer, tableName, getPKString(uri), node);
                    }
                })
                .build();
    }

    @DELETE
    @Path("/" + IDENTIFIERS_MULTI)
    public Response deleteEntity(@Context HttpServletRequest request,
                                 @PathParam("entity") String entity,
                                 @Context final UriInfo uri) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        try {
            reqs.restDMLService.delete(tableName, getPKString(uri));
            return RestResponseBuilder
                    .forRequest(request)
                    .status(Response.Status.NO_CONTENT)
                    .build();
        } catch (Exception e) {
            throw RestResponseBuilder.forRequest(request).wrapException(e);
        }
    }
    
    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response patchEntity(@Context HttpServletRequest request,
            @PathParam("entity") String entity,
            final byte[] entityBytes) {
        final TableName tableName = parseTableName(request, entity);
        checkTableAccessible(reqs.securityService, request, tableName);
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        final JsonNode node = readTree(entityBytes);
                        reqs.restDMLService.upsert(writer, tableName, node);
                    }
                })
                .build();
    }
}
