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
package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

import java.util.List;
import java.util.Properties;
import java.io.File;

public class RulesTestContext extends SchemaRulesContext
{
    protected RulesTestContext() {
    }

    public static RulesTestContext create(AkibanInformationSchema ais,
                                          File statsFile, boolean statsIgnoreMissingIndexes,
                                          List<? extends BaseRule> rules, 
                                          Properties properties)
            throws Exception {
        RulesTestContext context = new RulesTestContext();
        context.initProperties(properties);
        context.initRules(rules);
        RulesTestHelper.ensureFieldAssociations(ais);
        context.initAIS(ais);
        context.initCostEstimator(new TestCostEstimator(ais, context.getSchema(), 
                                                        statsFile, statsIgnoreMissingIndexes,
                                                        properties));
        context.initPipelineConfiguration(new PipelineConfiguration());

        TypesRegistryServiceImpl typesRegistry = new TypesRegistryServiceImpl();
        typesRegistry.start();
        context.initTypesRegistry(typesRegistry);
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        context.initTypesTranslator(typesTranslator);

        context.initDone();
        return context;
    }

    @Override
    public String getDefaultSchemaName() {
        return OptimizerTestBase.DEFAULT_SCHEMA;
    }

}
