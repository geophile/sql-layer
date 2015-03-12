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
package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;


import java.util.HashMap;
import java.util.Map;

public class PlanGenerator
{
    private Schema schema;
    private Map<Table,Operator> scanPlans = new HashMap<>();
    private Map<Table,Operator> branchPlans = new HashMap<>();
    private Map<Table,Operator> ancestorPlans = new HashMap<>();

    public PlanGenerator(AkibanInformationSchema ais) {
        this.schema = SchemaCache.globalSchema(ais);
    }

    public Schema getSchema() {
        return schema;
    }

    // TODO: Can narrow synchronization to plans and schema.

    public synchronized Operator generateScanPlan(Table table) {
        Operator plan = scanPlans.get(table);
        if (plan != null) return plan;

        plan =  com.foundationdb.sql.optimizer.rule.PlanGenerator.generateScanPlan(schema.ais(), table);
        
        scanPlans.put(table, plan);
        return plan;
    }

    public synchronized Operator generateBranchPlan(Table table) {
        Operator plan = branchPlans.get(table);
        if (plan != null) return plan;
        
        plan =  com.foundationdb.sql.optimizer.rule.PlanGenerator.generateBranchPlan(schema.ais(), table);

        branchPlans.put(table, plan);
        return plan;
    }

    public Operator generateBranchPlan(Table table, Operator scan, RowType scanType) {
        // No caching possible.
        return com.foundationdb.sql.optimizer.rule.PlanGenerator.generateBranchPlan(table, scan, scanType);
    }
    
    public Operator generateAncestorPlan (Table table) {
        Operator plan = ancestorPlans.get(table);
        if (plan != null) return plan;
        
        plan = com.foundationdb.sql.optimizer.rule.PlanGenerator.generateAncestorPlan(schema.ais(), table);
        ancestorPlans.put(table, plan);
        return plan;
    }
}
