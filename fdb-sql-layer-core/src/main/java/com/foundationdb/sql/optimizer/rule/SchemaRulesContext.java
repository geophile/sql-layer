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

import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.common.types.TypesTranslator;

/** The context associated with an AIS schema. */
public abstract class SchemaRulesContext extends RulesContext
{
    private Schema schema;

    private CostEstimator costEstimator;
    private PipelineConfiguration pipelineConfiguration;
    private TypesRegistryService typesRegistry;
    private TypesTranslator typesTranslator;

    protected SchemaRulesContext() {
    }

    protected void initAIS(AkibanInformationSchema ais) {
        schema = SchemaCache.globalSchema(ais);
    }
     
    protected void initCostEstimator(CostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    protected void initPipelineConfiguration(PipelineConfiguration pipelineConfiguration) {
        this.pipelineConfiguration = pipelineConfiguration;
    }

    protected void initTypesRegistry(TypesRegistryService typesRegistry) {
        this.typesRegistry = typesRegistry;
    }

    protected void initTypesTranslator(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (schema != null) : "initSchema() not called";
        assert (costEstimator != null) : "initCostEstimator() not called";
        assert (pipelineConfiguration != null) : "initPipelineConfiguration() not called";
        assert (typesRegistry != null) : "initTypesRegistry() not called";
        assert (typesTranslator != null) : "initTypesTranslator() not called";
    }

    public Schema getSchema() {
        return schema;
    }

    public AkibanInformationSchema getAIS() {
        return schema.ais();
    }

    public abstract String getDefaultSchemaName();

    public CostEstimator getCostEstimator() {
        return costEstimator;
    }

    public PipelineConfiguration getPipelineConfiguration() {
        return pipelineConfiguration;
    }

    public TypesRegistryService getTypesRegistry() {
        return typesRegistry;
    }

    public TypesTranslator getTypesTranslator() {
        return typesTranslator;
    }

    public PhysicalResultColumn getResultColumn(ResultField field) {
        return new PhysicalResultColumn(field.getName());
    }

}
