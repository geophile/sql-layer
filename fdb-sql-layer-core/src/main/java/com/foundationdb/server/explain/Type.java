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
package com.foundationdb.server.explain;

/**
 * <b>Type</b>
 * Reflects object's type/class
 * Can be useful for those formatters that want to format
 * scan_xyz_operator's explainer differently from insert_operator's explainer, 
 * or to format functions differently from binary operators (such as +, -, LIKE, ILIKE, etc)
 * 
 * <b>GeneralType</b>
 * Refects each class of Explainers
 * 
 */
public enum Type
{
    // AGGREGATOR
    //--------------------------------------------------------------------------
    AGGREGATOR(GeneralType.AGGREGATOR),
    
    // EXPRESSION
    //-------------------------------------------------------------------------- 
    FIELD(GeneralType.EXPRESSION),
    FUNCTION(GeneralType.EXPRESSION),
    BINARY_OPERATOR(GeneralType.EXPRESSION),
    SUBQUERY(GeneralType.EXPRESSION),
    LITERAL(GeneralType.EXPRESSION),
    VARIABLE(GeneralType.EXPRESSION),

    // OPERATORS
    //--------------------------------------------------------------------------
    SCAN_OPERATOR(GeneralType.OPERATOR),
    LOOKUP_OPERATOR(GeneralType.OPERATOR),
    COUNT_OPERATOR(GeneralType.OPERATOR),
    DUI(GeneralType.OPERATOR), // delete/update/insert
    DISTINCT(GeneralType.OPERATOR),
    FLATTEN_OPERATOR(GeneralType.OPERATOR),
    PRODUCT_OPERATOR(GeneralType.OPERATOR),
    LIMIT_OPERATOR(GeneralType.OPERATOR),
    NESTED_LOOPS(GeneralType.OPERATOR),
    IF_EMPTY(GeneralType.OPERATOR),
    UNION(GeneralType.OPERATOR),
    EXCEPT(GeneralType.OPERATOR),
    INTERSECT(GeneralType.OPERATOR),
    SORT(GeneralType.OPERATOR),
    FILTER(GeneralType.OPERATOR),
    PROJECT(GeneralType.OPERATOR),
    SELECT_HKEY(GeneralType.OPERATOR),
    AGGREGATE(GeneralType.OPERATOR),
    ORDERED(GeneralType.OPERATOR),
    BLOOM_FILTER(GeneralType.OPERATOR),
    BUFFER_OPERATOR(GeneralType.OPERATOR),
    HKEY_OPERATOR(GeneralType.OPERATOR),
    HASH_JOIN(GeneralType.OPERATOR),
    
    // PROCEDURE    
    //--------------------------------------------------------------------------
    PROCEDURE(GeneralType.PROCEDURE),
    
    // ROWTYPE    
    //--------------------------------------------------------------------------
    ROWTYPE(GeneralType.ROWTYPE),
    
    // ROW
    //--------------------------------------------------------------------------
    ROW(GeneralType.ROW),
    
    // SCALAR 
    //--------------------------------------------------------------------------
    FLOATING_POINT(GeneralType.SCALAR_VALUE),
    EXACT_NUMERIC(GeneralType.SCALAR_VALUE),
    STRING(GeneralType.SCALAR_VALUE),
    BOOLEAN(GeneralType.SCALAR_VALUE),
    
    // EXTRA_INFO
    //--------------------------------------------------------------------------
    EXTRA_INFO(GeneralType.EXTRA_INFO),
    ;
    
    private final GeneralType generalType;
    private Type (GeneralType type)
    {
        generalType = type;
    }
    
    public GeneralType generalType ()
    {
        return generalType;
    }
    
    public enum GeneralType
    {
        AGGREGATOR,
        EXPRESSION,
        OPERATOR,
        PROCEDURE,
        SCALAR_VALUE,
        ROWTYPE,
        ROW,
        EXTRA_INFO
    }
}
