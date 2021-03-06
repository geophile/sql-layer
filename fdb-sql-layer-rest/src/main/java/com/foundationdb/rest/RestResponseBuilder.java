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

import com.foundationdb.rest.resources.ResourceHelper;
import com.foundationdb.server.Quote;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.sql.embedded.JDBCException;
import com.foundationdb.util.AkibanAppender;
import com.fasterxml.jackson.core.JsonParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class RestResponseBuilder {
    public interface BodyGenerator {
        public void write(PrintWriter writer) throws Exception;
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final Map<Class,Response.Status> EXCEPTION_STATUS_MAP = buildExceptionStatusMap();
    private static final Logger LOG = LoggerFactory.getLogger(RestResponseBuilder.class);

    private final HttpServletRequest request;
    private final boolean isJsonp;
    private BodyGenerator outputGenerator;
    private String outputBody;
    private String jsonp;
    private int status;
    private MediaType type;


    public RestResponseBuilder(HttpServletRequest request, String jsonp) {
        this.request = request;
        this.jsonp = jsonp;
        this.isJsonp = jsonp != null;
        this.status = Response.Status.OK.getStatusCode();
    }

    public static RestResponseBuilder forRequest(HttpServletRequest request) {
        return new RestResponseBuilder(request, request.getParameter(ResourceHelper.JSONP_ARG_NAME));
    }

    public RestResponseBuilder status(Response.Status status) {
        this.status = status.getStatusCode();
        return this;
    }
    
    public RestResponseBuilder type(MediaType type) {
        this.type = type;
        return this;
    }

    public RestResponseBuilder body(String outputBody) {
        this.outputBody = outputBody;
        this.outputGenerator = null;
        return this;
    }

    public RestResponseBuilder body(ErrorCode code, String message) {
        body(formatErrorWithJsonp(code.getFormattedValue(), message));
        return this;
    }

    public RestResponseBuilder body(BodyGenerator outputGenerator) {
        this.outputBody = null;
        this.outputGenerator = outputGenerator;
        return this;
    }

    public Response build() {
        if(outputBody == null && outputGenerator == null && jsonp == null) {
            status(Response.Status.NO_CONTENT);
        }
        if (isJsonp) {
            status(Response.Status.OK);
        }
        Response.ResponseBuilder builder;
        if (this.status == Response.Status.NO_CONTENT.getStatusCode()) {
            builder = Response.status(status).type((MediaType)null);
        } else {
            builder = Response.status(status).entity(createStreamingOutput());
        }
        if(isJsonp) {
            builder.type(ResourceHelper.APPLICATION_JAVASCRIPT_TYPE);
        } else if (type != null) {
            builder.type(type);
        }
        return builder.build();
    }
    
    public static void formatJsonError(StringBuilder builder, String code, String message, String note) {
        builder.append("{\"code\":\"");
        builder.append(code);
        builder.append("\", \"message\":\"");
        Quote.JSON_QUOTE.append(AkibanAppender.of(builder), message);
        if (note != null) {
            builder.append("\", \"note\":\"");
            Quote.JSON_QUOTE.append(AkibanAppender.of(builder), note);
        }
        builder.append("\"}");
    }


    private String formatErrorWithJsonp(String code, String message) {
        StringBuilder builder = new StringBuilder();
        if(isJsonp) {
            builder.append(jsonp);
            builder.append('(');
        }
        formatJsonError(builder, code, message, null);
        if(isJsonp) {
            builder.append(')');
        }
        builder.append('\n');
        return builder.toString();
    }

    public WebApplicationException wrapException(Throwable e) {
        Throwable cause = e instanceof JDBCException ? e.getCause() : null;
        if (cause instanceof InvalidOperationException) {
            e = cause;
        }
        final ErrorCode code = ErrorCode.getCodeForRESTException(e);
        Response.Status status = EXCEPTION_STATUS_MAP.get(e.getClass());
        if(status == null) {
            status = Response.Status.CONFLICT;
        }
        code.logAtImportance(
                LOG,
                LOG.isDebugEnabled() ? "Exception from request(method: {}, url: {}, params: {})"
                                     : "Exception from request(method: {}, url: {}, params: {}): {}",
                request.getMethod(), request.getRequestURL(), request.getQueryString(),
                e
        );
        String exMsg = (e.getMessage() != null) ? e.getMessage() : e.getClass().getName();
        return new WebApplicationException(
                Response.status(status)
                        .entity(formatErrorWithJsonp(code.getFormattedValue(), exMsg))
                        .type(isJsonp ? ResourceHelper.APPLICATION_JAVASCRIPT_TYPE : MediaType.APPLICATION_JSON_TYPE)
                        .build()
        );
    }

    private StreamingOutput createStreamingOutput() {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream output)  {
                try {
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, UTF8), false);
                    if(isJsonp) {
                        writer.write(jsonp);
                        writer.write('(');
                    }
                    if(outputGenerator != null) {
                        outputGenerator.write(writer);
                    } else if(outputBody != null) {
                        writer.write(outputBody);
                    }
                    if(isJsonp) {
                        writer.write(')');
                    }
                    writer.write('\n');
                    writer.flush();
                    writer.close();
                } catch(Throwable t) {
                    throw wrapException(t);
                }
            }
        };
    }

    private static Map<Class, Response.Status> buildExceptionStatusMap() {
        Map<Class, Response.Status> map = new HashMap<>();
        map.put(NoSuchTableException.class, Response.Status.NOT_FOUND);
        map.put(NoSuchRoutineException.class, Response.Status.NOT_FOUND);
        map.put(JsonParseException.class, Response.Status.BAD_REQUEST);
        return map;
    }
}
