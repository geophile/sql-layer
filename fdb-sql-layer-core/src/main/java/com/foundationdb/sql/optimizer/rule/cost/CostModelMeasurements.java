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
package com.foundationdb.sql.optimizer.rule.cost;

public interface CostModelMeasurements
{
    // From SelectCT
    final double SELECT_PER_ROW = 0.22;
    // From ProjectCT
    final double PROJECT_PER_ROW = 0.26;
    // From ExpressionCT
    final double EXPRESSION_PER_FIELD = 0.6;
    // From SortCT
    final double SORT_SETUP = 64;
    final double SORT_PER_ROW = 10;
    final double SORT_MIXED_MODE_FACTOR = 1.5;
    // From FlattenCT
    final double FLATTEN_OVERHEAD = 49;
    final double FLATTEN_PER_ROW = 41;
    // From MapCT
    final double MAP_PER_ROW = 0.15;
    // From ProductCT
    final double PRODUCT_PER_ROW = 40;
    // From SortWithLimitCT
    final double SORT_LIMIT_PER_ROW = 1;
    final double SORT_LIMIT_PER_FIELD_FACTOR = 0.2;
    // From DistinctCT
    final double DISTINCT_PER_ROW = 6;
    // From IntersectCT
    final double INTERSECT_PER_ROW = 0.25;
    // Also based on IntersectIT, since Union_Ordered works very similarly to Intersect_Ordered.
    final double UNION_PER_ROW = 0.25;
    // From HKeyUnionCT
    final double HKEY_UNION_PER_ROW = 2;
    // From Select_BloomFilterCT.
    final double BLOOM_FILTER_LOAD_PER_ROW = 0.24;
    final double BLOOM_FILTER_SCAN_PER_ROW = 0.39;
    final double BLOOM_FILTER_SCAN_SELECTIVITY_COEFFICIENT = 7.41;
    // From Select_HashTableCT
    final double HASH_TABLE_LOAD_PER_ROW = .26 ;
    final double HASH_TABLE_SCAN_PER_ROW =  .18;
    final double HASH_TABLE_DIFF_PER_JOIN = .144;
    final double HASH_TABLE_COLUMN_COUNT_OFFSET = .0001;

}
