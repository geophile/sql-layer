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
package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.sql.embedded.EmbeddedOperatorCompiler;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.CreateTableAsRules;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.sql.server.ServerSession;

import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.sql.optimizer.rule.DefaultRules.DEFAULT_RULES;


public class CreateAsCompiler extends EmbeddedOperatorCompiler {

    boolean removeTableSources;

    public CreateAsCompiler(ServerSession server, StoreAdapter adapter, boolean removeTableSources, AkibanInformationSchema ais) {
        initProperties(server.getCompilerProperties());
        initAIS(ais, server.getDefaultSchemaName());
        initParser(server.getParser());
        initCostEstimator(server.costEstimator(this, adapter.getKeyCreator()));
        initPipelineConfiguration(server.getPipelineConfiguration());
        initTypesRegistry(server.typesRegistryService());
        initTypesTranslator(server.typesTranslator());
        server.getBinderContext().setBinderAndTypeComputer(binder, typeComputer);
        server.setAttribute("compiler", this);
        this.initDone();

        if(removeTableSources) {
            List<BaseRule> newRules = new ArrayList<>();
            for(BaseRule rule : DEFAULT_RULES){
                newRules.add(rule);
                if(rule instanceof ASTStatementLoader){
                    CreateTableAsRules newRule = new CreateTableAsRules();
                    newRules.add(newRule);
                }
            }
           initRules(newRules);
        }
        this.removeTableSources = removeTableSources;

    }

    @Override
    protected void initCostEstimator(CostEstimator costEstimator) {
        super.initCostEstimator(costEstimator);
        List<BaseRule> rules = DEFAULT_RULES;
        if(removeTableSources) {
            CreateTableAsRules newRule = new CreateTableAsRules();
            rules.add(newRule);
        }
        initRules(rules);
    }

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }
}

