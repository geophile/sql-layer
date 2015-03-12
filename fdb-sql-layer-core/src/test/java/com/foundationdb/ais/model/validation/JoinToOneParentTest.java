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
package com.foundationdb.ais.model.validation;

import java.util.Collection;
import java.util.LinkedList;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.types.service.TestTypesRegistry;


public class JoinToOneParentTest {
    private LinkedList<AISValidation>validations;
    private TestAISBuilder builder; 

    @Before 
    public void createValidations() {
        validations = new LinkedList<>();
        validations.add(AISValidations.JOIN_TO_ONE_PARENT);
        
        builder = new TestAISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "customer");
        builder.column("schema", "customer", "customer_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "customer", "customer_name", 1, "MCOMPAT", "varchar", 64L, null, false);
        builder.pk("schema", "customer");
        builder.indexColumn("schema", "customer", Index.PRIMARY, "customer_id", 0, true, null);
        builder.table("schema", "order");
        builder.column("schema", "order", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "order", "customer_id", 1, "MCOMPAT", "int", false);
        builder.column("schema", "order", "order_date", 2, "MCOMPAT", "int", false);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema");
        builder.addJoinToGroup("group", "co", 0);
        builder.groupingIsComplete();
        
    }

    @Test
    public void testValidJoins() {
        Assert.assertEquals(0, 
                builder.akibanInformationSchema().validate(validations).failures().size());
    }
    
    @Test 
    public void testTwoJoinsToOneParent() {
        builder.joinTables("co2", "schema", "customer", "schema", "order");
        builder.joinColumns("co2", "schema", "customer", "customer_id", "schema", "order", "customer_id");
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }
    
    @Test
    public void testTwoJoinsToTwoParents() {
        builder.table("schema", "address");
        builder.column("schema", "address", "order_id", 0, "MCOMPAT", "int", false);
        builder.column("schema", "address", "customer_id", 1, "MCOMPAT", "int", false);
        builder.joinTables("ca", "schema", "customer", "schema", "address");
        builder.joinColumns("ca", "schema", "customer", "customer_id", "schema", "address", "customer_id");
        builder.joinTables("oa", "schema", "order", "schema", "address");
        builder.joinColumns("oa", "schema", "order", "order_id", "schema", "address", "order_id");
        builder.basicSchemaIsComplete();
        builder.addJoinToGroup("group", "ca", 0);
        //builder.addJoinToGroup("group", "oa", 0);
        builder.groupingIsComplete();
        Collection<AISValidationFailure> failures = builder.akibanInformationSchema().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_MULTIPLE_PARENTS, fail.errorCode());
    }
}
