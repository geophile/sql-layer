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

import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.sql.optimizer.plan.AST;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.SchemaRulesContext;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;
import static com.foundationdb.sql.optimizer.rule.DefaultRules.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.compiler.BooleanNormalizer;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.NodeFactory;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserContext;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.error.SQLParserInternalException;

import com.foundationdb.sql.IncomparableException;
import java.util.List;

/**
 * Compile SQL statements into operator trees.
 */ 
public class OperatorCompiler extends SchemaRulesContext
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected FunctionsTypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected DistinctEliminator distinctEliminator;

    protected OperatorCompiler() {
    }

    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        initAIS(ais);
        binder = new AISBinder(ais, defaultSchemaName);
    }

    protected void initParser(SQLParser parser) {
        parserContext = parser;
        BindingNodeFactory.wrap(parser);
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    @Override
    protected void initTypesRegistry(TypesRegistryService typesRegistry) {
        super.initTypesRegistry(typesRegistry);
        typeComputer = new FunctionsTypeComputer(typesRegistry);
        binder.setFunctionDefined(new AISBinder.FunctionDefined() {
                @Override
                public boolean isDefined(String name) {
                    return (getTypesRegistry().getFunctionKind(name) != null);
                }
            });
    }

    @Override
    protected void initCostEstimator(CostEstimator costEstimator) {
        super.initCostEstimator(costEstimator);

        List<BaseRule> rules = DEFAULT_RULES;
        initRules(rules);
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (parserContext != null) : "initParser() not called";
        binder.setResultColumnsAvailableBroadly(Boolean.parseBoolean(getProperty("resultColumnsAvailableBroadly", "false")));
    }

    /** Compile a statement into an operator tree. */
    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params) {
        return compile(stmt, params, new PlanContext(this));
    }

    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params,
                                 PlanContext plan) {
        stmt = bindAndTransform(stmt); // Get into standard form.
        plan.setPlan(new AST(stmt, params));
        applyRules(plan);
        return (BasePlannable)plan.getPlan();
    }

    /** Apply AST-level transformations before rules. */
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            binder.bind(stmt);
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            typeComputer.compute(stmt);
            stmt = subqueryFlattener.flatten(stmt);
            // TODO: Temporary for safety.
            if (Boolean.parseBoolean(getProperty("eliminate-distincts", "true")))
                stmt = distinctEliminator.eliminate(stmt);
            return stmt;
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    @Override
    public String getDefaultSchemaName() {
        return binder.getDefaultSchemaName();
    }

}
