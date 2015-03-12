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
 * Identify each attribute
 */
public enum Label
{
    // CHILD(REN) OPERATION(S)
   //--------------------------------------------------------------------------
    AGGREGATORS(Category.CHILD),
    INPUT_OPERATOR(Category.CHILD),
    OPERAND(Category.CHILD), // function operand, operands in general
    PROJECTION(Category.CHILD), // list of a expressions
    PREDICATE(Category.CHILD),
    EXPRESSIONS(Category.CHILD),
    BLOOM_FILTER(Category.CHILD),
    HIGH_COMPARAND(Category.CHILD),
    LOW_COMPARAND(Category.CHILD),
    EQUAL_COMPARAND(Category.CHILD),
    
    
    // COST
    //--------------------------------------------------------------------------
    COST(Category.COST),
    
    // DESCRIPTION (may or may not needed)
    //--------------------------------------------------------------------------
    POSITION(Category.DESCRIPTION),
    BINDING_POSITION(Category.DESCRIPTION),
    EXTRA_TAG(Category.DESCRIPTION), // extra info
    INFIX_REPRESENTATION(Category.DESCRIPTION),
    ASSOCIATIVE(Category.DESCRIPTION),
    INDEX(Category.DESCRIPTION),
    PIPELINE(Category.DESCRIPTION),
    DEPTH(Category.DESCRIPTION),
    
    // IDENTIFIER
    //--------------------------------------------------------------------------
    NAME(Category.IDENTIFIER),
    START_TABLE(Category.IDENTIFIER),
    STOP_TABLE(Category.IDENTIFIER),
    TABLE_SCHEMA(Category.IDENTIFIER),
    TABLE_NAME(Category.IDENTIFIER),
    TABLE_CORRELATION(Category.IDENTIFIER),
    COLUMN_NAME(Category.IDENTIFIER),
    INDEX_NAME(Category.IDENTIFIER),
    
    // OPTION
    //--------------------------------------------------------------------------
    INPUT_PRESERVATION(Category.OPTION),
    GROUPING_OPTION(Category.OPTION),
    FLATTEN_OPTION(Category.OPTION), // keep parent, etc 
    SORT_OPTION(Category.OPTION),
    SCAN_OPTION(Category.OPTION), // full/deep.shallow, etc
    LIMIT(Category.OPTION),
    PROJECT_OPTION(Category.OPTION), // has a table or not
    JOIN_OPTION(Category.OPTION), // INNER, LEFT, etc
    ORDERING(Category.OPTION), // ASC or DESC
    INDEX_KIND(Category.OPTION),
    INDEX_SPATIAL_DIMENSIONS(Category.OPTION),
    ORDER_EFFECTIVENESS(Category.OPTION),
    USED_COLUMNS(Category.OPTION),
    NUM_SKIP(Category.OPTION),
    NUM_COMPARE(Category.OPTION),
    SET_OPTION(Category.OPTION),
    PROCEDURE_CALLING_CONVENTION(Category.OPTION),
    PROCEDURE_IMPLEMENTATION(Category.OPTION),

    // TYPE DESCRIPTION
    //--------------------------------------------------------------------------
    INNER_TYPE(Category.TYPE_DESCRIPTION),
    PARENT_TYPE(Category.TYPE_DESCRIPTION),
    CHILD_TYPE(Category.TYPE_DESCRIPTION),
    LEFT_TYPE(Category.TYPE_DESCRIPTION),
    RIGHT_TYPE(Category.TYPE_DESCRIPTION),
    KEEP_TYPE(Category.TYPE_DESCRIPTION),
    OUTER_TYPE(Category.TYPE_DESCRIPTION),
    PRODUCT_TYPE(Category.TYPE_DESCRIPTION),
    INPUT_TYPE(Category.TYPE_DESCRIPTION),
    OUTPUT_TYPE(Category.TYPE_DESCRIPTION),
    TABLE_TYPE(Category.TYPE_DESCRIPTION),
    ROWTYPE(Category.TYPE_DESCRIPTION),
    DINSTINCT_TYPE(Category.TYPE_DESCRIPTION),
    ANCESTOR_TYPE(Category.TYPE_DESCRIPTION),
    PREDICATE_ROWTYPE(Category.TYPE_DESCRIPTION),
    ;


    public enum Category
    {
        CHILD, // operand for expressions, or input operator for operator
        COST,
        DESCRIPTION, //extra info (may not needed by the caller        
        IDENTIFIER,        
        OPTION,        
        TYPE_DESCRIPTION,
    }

    public Category getCategory ()
    {
        return category;
    }

    private Label (Category g)
    {
        this.category = g;
    }

    private final Category category;
}
