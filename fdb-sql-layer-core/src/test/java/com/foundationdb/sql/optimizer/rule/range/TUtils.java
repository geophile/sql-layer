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
package com.foundationdb.sql.optimizer.rule.range;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.FunctionCondition;
import com.foundationdb.sql.optimizer.plan.LogicalFunctionCondition;
import com.foundationdb.sql.optimizer.plan.TableNode;
import com.foundationdb.sql.optimizer.plan.TableSource;
import com.foundationdb.sql.optimizer.plan.TableTree;

import java.util.Arrays;
import java.util.Collections;

final class TUtils {

    public static ConstantExpression constant(String value) {
        return new ConstantExpression(value, MString.VARCHAR.instance(true));
    }

    public static ConstantExpression constant(long value) {
        return new ConstantExpression(value, MNumeric.BIGINT.instance(true));
    }

    public static ConditionExpression compare(ColumnExpression column, Comparison comparison, ConstantExpression value) {
        return new ComparisonCondition(comparison, column, value, null, null, null);
    }

    public static ConditionExpression compare(ConstantExpression value, Comparison comparison, ColumnExpression column) {
        return new ComparisonCondition(comparison, value, column, null, null, null);
    }

    public static ConditionExpression isNull(ColumnExpression column) {
        return new FunctionCondition("isNull", Collections.<ExpressionNode>singletonList(column), null, null, null);
    }

    public static ConditionExpression or(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("or", Arrays.asList(left, right), null, null, null);
    }

    public static ConditionExpression and(ConditionExpression left, ConditionExpression right) {
        return new LogicalFunctionCondition("and", Arrays.asList(left, right), null, null, null);
    }

    public static ConditionExpression not(ConditionExpression expression) {
        return new LogicalFunctionCondition("not", Arrays.asList(expression), null, null, null);
    }

    public static ConditionExpression sin(ColumnExpression column) {
        return new FunctionCondition("sin", Collections.<ExpressionNode>singletonList(column), null, null, null);
    }

    public static RangeSegment segment(RangeEndpoint start, RangeEndpoint end) {
        return new RangeSegment(start, end);
    }

    public static RangeEndpoint inclusive(long value) {
        return RangeEndpoint.inclusive(constant(value));
    }

    public static RangeEndpoint exclusive(long value) {
        return RangeEndpoint.exclusive(constant(value));
    }

    public static RangeEndpoint inclusive(String value) {
        return RangeEndpoint.inclusive(constant(value));
    }

    public static RangeEndpoint exclusive(String value) {
        return RangeEndpoint.exclusive(constant(value));
    }

    public static RangeEndpoint nullInclusive(String value) {
        return RangeEndpoint.nullInclusive(constant(value));
    }

    public static RangeEndpoint nullExclusive(String value) {
        return RangeEndpoint.nullExclusive(constant(value));
    }

    public static final ColumnExpression lastName;
    public static final ColumnExpression firstName;

    static {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        AkibanInformationSchema ais = AISBBasedBuilder.create("s", typesTranslator)
            .table("t1").colString("first_name", 32).colString("last_name", 32)
            .ais();
        Table table = ais.getTable("s", "t1");
        TableNode node = new TableNode(table, new TableTree());
        TableSource source = new TableSource(node, true, "t1");
        lastName = new ColumnExpression(source, table.getColumn("first_name"));
        firstName = new ColumnExpression(source, table.getColumn("last_name"));
    }

    private TUtils() {}
}
