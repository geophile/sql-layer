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

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

public class JoinToParentPKTest {
    private LinkedList<AISValidation>validations;
    private NewAISBuilder builder;

    @Before 
    public void createValidations () {
        validations = new LinkedList<>();
        validations.add(AISValidations.JOIN_TO_PARENT_PK);
        validations.add(AISValidations.JOIN_COLUMN_TYPES_MATCH);

        builder = AISBBasedBuilder.create("test", MTypesTranslator.INSTANCE);
        builder.table("t1").colInt("c1").colString("c2", 10).pk("c1");
        builder.table("t2").colInt("c1").colString("c2", 10).pk("c1", "c2");
        builder.table("t3").colInt("c1").colString("c2", 10);
    }
    
    @Test
    public void joinOneColumnValid() {
        builder.table("j1").colInt("c1").colInt("c2").pk("c1").joinTo("t1").on("c2", "c1");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }
    
    @Test
    public void joinTwoColumnValid() {
        builder.table("j2").colInt("c1").colString("c2", 10).pk("c1").joinTo("t2").on("c1", "c1").and("c2", "c2");
        Assert.assertEquals(0, 
                builder.unvalidatedAIS().validate(validations).failures().size());
    }

    @Test
    public void joinNoPKFailed() {
        builder.table("j3").colInt("c1").joinTo("t3").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_PARENT_NO_PK, fail.errorCode());
    }
    
    @Test
    public void joinOneToTwoMismatch() {
        builder.table("j4").colInt("c1").joinTo("t2").on("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinTwoToOneMismatch() { 
        builder.table("j5").colInt("c1").colString("c2", 10).joinTo("t1").on("c1","c1").and("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_COLUMN_MISMATCH, fail.errorCode());
    }
    
    @Test
    public void joinColumnsMismatch () { 
        builder.table("j6").colInt("c1").colString("c2", 10).joinTo("t2").on("c2", "c1").and("c1", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
        
    }
    
    @Test
    public void joinOrderMismatch() {
        builder.table("j7").colInt("c1").colString("c2", 10).joinTo("t2").on("c2", "c2").and("c1", "c1");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(2, failures.size());
    }
    
    @Test
    public void joinToNonPKColumns() {
        builder.table("j8").colInt("c1").colString("c2", 10).joinTo("t1").on("c2", "c2");
        Collection<AISValidationFailure> failures = builder.unvalidatedAIS().validate(validations).failures();
        Assert.assertEquals(1, failures.size());
        AISValidationFailure fail = failures.iterator().next();
        Assert.assertEquals(ErrorCode.JOIN_TO_WRONG_COLUMNS, fail.errorCode());

    }
}
