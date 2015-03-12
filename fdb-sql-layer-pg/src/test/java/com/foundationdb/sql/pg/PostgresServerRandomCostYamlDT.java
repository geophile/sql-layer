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
package com.foundationdb.sql.pg;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;

import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.optimizer.rule.cost.RandomCostModelService;
import org.junit.Test;

import java.net.URL;

/**
 * Run tests specified as YAML files that end with the .yaml extension.  By
 * default, searches for files recursively in the yaml resource directory,
 * running tests for files that start with 'test-'.  Tests will be run with
 * Random Cost Model in order to cause alternative operator plans to test correctness
 */
public class PostgresServerRandomCostYamlDT extends PostgresServerMiscYamlIT
{
    public PostgresServerRandomCostYamlDT(String caseName, URL url) {
        super(caseName, url);
    }

    @Override
    protected boolean isRandomCost(){
        return true;
    }
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(CostModelFactory.class, RandomCostModelService.class);
        }

    @Test
    public void testYaml() throws Exception {
        boolean  success = false;
        try {
            ((RandomCostModelService) serviceManager().getServiceByClass(CostModelFactory.class)).reSeed();
            super.testYaml();
            success = true;
        } finally {
            if(success == false){
                System.err.printf("\nFailed when ran with random seed: %d \n",
                        ((RandomCostModelService) serviceManager().getServiceByClass(CostModelFactory.class)).getSeed()
                );
            }
        }
    }

}
