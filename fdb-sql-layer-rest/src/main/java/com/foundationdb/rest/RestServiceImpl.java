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

import com.foundationdb.http.HttpConductor;
import com.foundationdb.rest.dml.RestDMLService;
import com.foundationdb.rest.resources.*;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.google.inject.Inject;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.HashSet;
import java.util.Set;

public class RestServiceImpl implements RestService, Service {
    private final ConfigurationService configService;
	private final HttpConductor http;
    // Used by various resources
    private final ResourceRequirements reqs;

	private volatile ServletHolder servletHolder;
	
	private static final String RESOURCE_LIST = "fdbsql.rest.resource";
	

	@Inject
    public RestServiceImpl(ConfigurationService configService,
                           HttpConductor http,
                           RestDMLService restDMLService,
                           SessionService sessionService,
                           TransactionService transactionService,
                           SecurityService securityService,
                           DXLService dxlService,
                           Store store) {
        this.configService = configService;
		this.http = http;
        this.reqs = new ResourceRequirements(
            dxlService,
            restDMLService,
            securityService,
            sessionService,
            transactionService,
            store,
            configService
        );
    }

    @Override
    public String getContextPath() {
        return configService.getProperty("fdbsql.rest.context_path");
    }

	@Override
	public void start() {
		registerConnector(http);
	}

	@Override
	public void stop() {
        http.unregisterHandler(servletHolder);
        this.servletHolder = null;
	}

	@Override
	public void crash() {
		stop();
	}

	private void registerConnector(HttpConductor http) {
        String path = getContextPath() + "/*";
        servletHolder = new ServletHolder(new ServletContainer(createResourceConfigV1()));
        http.registerHandler(servletHolder, path);
	}

    private ResourceConfig createResourceConfigV1() {
        String resource_list = configService.getProperty(RESOURCE_LIST);

        
        Set<Object> resources = new HashSet<>();
        if (resource_list.contains("entity")) {
            resources.add(new EntityResource(reqs));
        }
        if (resource_list.contains("fulltext")) {
            resources.add(new FullTextResource(reqs));
        }
        if (resource_list.contains("procedurecall")) {
            resources.add(new ProcedureCallResource(reqs));
        }
        if (resource_list.contains("security")) {
            resources.add(new SecurityResource(reqs));
        }
        if (resource_list.contains("sql")) {
            resources.add(new SQLResource(reqs));
        }
        if (resource_list.contains("version")) {
            resources.add(new VersionResource(reqs));
        }
        if (resource_list.contains("view")) {
            resources.add(new ViewResource(reqs));
        }
        // This must be last to capture anything not handled above
        resources.add(new DefaultResource());


        ResourceConfig config = new ResourceConfig();
        config.registerInstances(resources);
        return config;
    }
}
