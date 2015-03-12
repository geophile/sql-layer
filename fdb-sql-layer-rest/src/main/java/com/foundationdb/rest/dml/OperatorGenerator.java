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
package com.foundationdb.rest.dml;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;

public abstract class OperatorGenerator {
    
    private Schema schema;
    private AkibanInformationSchema ais;
    private QueryContext queryContext;
    private TypesRegistryService typesRegistry;
    private TypesTranslator typesTranslator;
    
    public OperatorGenerator (AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = SchemaCache.globalSchema(ais);
        queryContext = new SimpleQueryContext(null);
    }
    
    public void setTypesRegistry(TypesRegistryService registryService) {
        this.typesRegistry = registryService;
    }
    
    public TypesTranslator getTypesTranslator() {
        return typesTranslator;
    }

    public void setTypesTranslator(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
    }

    public Schema schema() {
        return schema;
    }
    
    public AkibanInformationSchema ais() {
        return ais;
    }
    
    public QueryContext queryContext() {
        return queryContext;
    }
    
    public TypesRegistryService registryService() {
        return typesRegistry;
    }

    static class RowStream {
        Operator operator;
        RowType rowType;
    }
    
    protected Operator indexAncestorLookup(TableName tableName) {
        Table table = ais().getTable(tableName);
        return PlanGenerator.generateAncestorPlan(ais(), table);
    }
}
