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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Priorities;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * This blocks any requests with an unknown or blank referer.
 * There is a filter in Jersey for CSRF protection, but that one uses a custom header as opposed to the
 * Referer, which has had bugs: https://blog.whitehatsec.com/tag/redirect/
 * By default the only allowed referer is https://localhost.
 * Note that this does a STRICT check of the referer and any blank referers are considered an attack
 * Unfortunately some proxies, browser extensions, and privacy software removes the referer, so
 * this will not work for a two-tier situation with the browser talking directly to the rest service.
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class CsrfProtectionRefererFilter implements javax.servlet.Filter {
    private static final Logger logger = LoggerFactory.getLogger(CsrfProtectionRefererFilter.class);

    public static final String REFERER_HEADER = "Referer";
    public static final String ALLOWED_REFERERS_PARAM = "AllowedReferersInitParam";

    private List<URI> allowedReferers;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String allowedReferersConfigProperty = filterConfig.getInitParameter(ALLOWED_REFERERS_PARAM);
        allowedReferers = parseAllowedReferers(allowedReferersConfigProperty);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        if (servletRequest instanceof HttpServletRequest) {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            String referer = request.getHeader(REFERER_HEADER);
            if (isAllowedUri(allowedReferers, referer, "GET".equals(request.getMethod()))) {
                filterChain.doFilter(servletRequest,  servletResponse);
            } else {
                logger.debug("CSRF attempt blocked due to invalid referer uri; Request:{} Referer:{}",
                        request.getRequestURI(),
                        referer);
                ((HttpServletResponse)servletResponse).sendError(403,
                        "Referer not allowed");
            }
        } else {
            logger.error("Unexpected type of request: {} -- {}", servletRequest.getClass(), servletRequest);
        }
    }

    @Override
    public void destroy() {

    }

    public static List<URI> parseAllowedReferers(String allowedReferersConfigProperty) {
        if (allowedReferersConfigProperty == null || allowedReferersConfigProperty.isEmpty()) {
            throw new IllegalAllowedReferersException("must not be null or empty", allowedReferersConfigProperty);
        }
        String[] split = allowedReferersConfigProperty.split("\\,");
        List<URI> allowedReferers = new ArrayList<>();
        for (String allowedReferer : split) {
            if (allowedReferer == null || allowedReferer.isEmpty()) {
                continue;
            } else {
                if (allowedReferer.contains("*")) {
                    throw new IllegalAllowedReferersException("do not support regexs (*)", allowedReferer);
                }
                URI uri;
                try {
                    uri = new URI(allowedReferer);
                } catch (NullPointerException | IllegalArgumentException | URISyntaxException e) {
                    throw new IllegalAllowedReferersException("includes invalid referer", allowedReferer, e);
                }
                if (uri == null) {
                    throw new IllegalAllowedReferersException("must not be null", allowedReferer);
                }
                if (uri.getUserInfo() != null) {
                    throw new IllegalAllowedReferersException("must not contain user information", allowedReferer);
                }
                if (uri.getPath() != null && !uri.getPath().isEmpty() && !uri.getPath().equals("/")) {
                    throw new IllegalAllowedReferersException("do not support restricting by path", allowedReferer);
                }
                if (uri.getFragment() != null) {
                    throw new IllegalAllowedReferersException("do not support restricting by fragment", allowedReferer);
                }
                if (uri.getScheme() == null || (!uri.getScheme().equals("http") && !uri.getScheme().equals("https"))) {
                    throw new IllegalAllowedReferersException("must be http or https", allowedReferer);
                }
                if (uri.getAuthority() == null || uri.getHost() == null) {
                    throw new IllegalAllowedReferersException("must be hierarchical (e.g. http://example.com)", allowedReferer);
                }
                allowedReferers.add(uri);
            }
        }
        if (allowedReferers.isEmpty()) {
            throw new IllegalAllowedReferersException("Invalid List of allowed csrf referers must not be null or empty",
                    allowedReferersConfigProperty);
        }
        return allowedReferers;
    }

    public static boolean isAllowedUri(List<URI> allowedReferers, String referer, boolean isGetRequest) {
        if (referer == null || referer.isEmpty()) {
            return isGetRequest;
        }
        URI refererUri = URI.create(referer);
        for (URI uri : allowedReferers) {
            if (uri.getScheme().equals(refererUri.getScheme()) &&
                    uri.getHost().equals(refererUri.getHost()) &&
                    uri.getPort() == refererUri.getPort()) {
                return true;
            }
        }
        return false;
    }

    public static class IllegalAllowedReferersException extends IllegalArgumentException {
        public IllegalAllowedReferersException(String message, String referer) {
            super("CSRF allowed referers " + message + ": " + referer);
        }
        public IllegalAllowedReferersException(String message, String referer, Exception cause) {
            super("CSRF allowed referers " + message + ": " + referer, cause);
        }

    }
}
