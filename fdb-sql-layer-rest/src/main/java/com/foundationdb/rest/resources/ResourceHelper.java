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
import com.foundationdb.server.service.security.SecurityService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.security.Principal;

public class ResourceHelper {
    public static final Response FORBIDDEN_RESPONSE = Response.status(Response.Status.FORBIDDEN).build();

    // Standard but not otherwise defined
    public static final String APPLICATION_JAVASCRIPT = "application/javascript";
    public static final MediaType APPLICATION_JAVASCRIPT_TYPE = MediaType.valueOf(APPLICATION_JAVASCRIPT);

    // For @Produces argument
    public static final String MEDIATYPE_JSON_JAVASCRIPT = MediaType.APPLICATION_JSON + "," + APPLICATION_JAVASCRIPT;

    public static final String JSONP_ARG_NAME = "callback";

    public static final String IDENTIFIERS_MULTI = "{identifiers:.*}";

    public static String getSchema(HttpServletRequest request) {
        Principal user = request.getUserPrincipal();
        return (user == null) ? "" : user.getName();
    }

    public static TableName parseTableName(HttpServletRequest request, String name) {
        String schema = getSchema(request);
        return TableName.parse(schema, name);
    }

    public static void checkTableAccessible(SecurityService security, HttpServletRequest request, TableName name) {
        checkSchemaAccessible(security, request, name.getSchemaName());
    }

    public static void checkSchemaAccessible(SecurityService security, HttpServletRequest request, String schema) {
        if(!security.isAccessible(request.getUserPrincipal(), request.isUserInRole(SecurityService.ADMIN_ROLE), schema)) {
            throw new WebApplicationException(FORBIDDEN_RESPONSE);
        }
    }

    /** Expected to be used along with {@link #IDENTIFIERS_MULTI} */
    public static String getPKString(UriInfo uri) {
        String pks[] = uri.getPath(false).split("/");
        assert pks.length > 0: uri;
        return pks[pks.length - 1];
    }
}
