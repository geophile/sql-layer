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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.collation.AkCollator;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.*;

// Inspired by bug 1026668, but also tests various ways of enforcing distinctness, in addition to Distinct_Partial.

public class Distinct_Partial_CaseInsensitive_IT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "s", "t",
            "id int not null",
            "cs varchar(10)", // case sensitive
            "ci varchar(10) collate en_us_ci", // case insensitive
            "ns int"); // non-string
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        group = group(t);
        caseSensitiveCollator = tRowType.table().getColumn("cs").getCollator();
        caseInsensitiveCollator = tRowType.table().getColumn("ci").getCollator();
        db = new Row[]{
            row(t, 0L, "aa_cs", "aa_ci", 0),
            row(t, 1L, "bb_cs", "bb_ci", 0),
            row(t, 2L, "aA_cs", "aA_ci", 0),
            row(t, 3L, "bB_cs", "bB_ci", 0),
            row(t, 4L, "Aa_cs", "Aa_ci", 0),
            row(t, 5L, "Bb_cs", "Bb_ci", 0),
            row(t, 6L, "AA_cs", "AA_ci", 0),
            row(t, 7L, "BB_cs", "BB_ci", 0),
            // make sure that all columns have to be examined
            row(t, 7L, "x", "x", 0),
            row(t, 8L, "x", "x", 0),
            row(t, 9L, "x", "x", 0),
            row(t, 10L, "x", "x", 0),
        };
        use(db);
    }

    @Test
    public void testCaseSensitiveUsingSortTree()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        Operator plan =
            sort_General(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCaseInsensitiveUsingSortTree()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper = Arrays.asList(toUpper(field(projectRowType, 0)));
        Operator plan =
            project_DefaultTest(
                sort_General(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES),
                projectRowType,
                convertToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNonStringUsingSortTree()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            sort_General(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES);
        Row[] expected = new Row[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMultipleColumnsUsingSortTree()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_DefaultTest(
                sort_General(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES),
                projectRowType,
                convertCaseInsensitiveToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCaseSensitiveUsingSortInsertionLimited()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        Operator plan =
            sort_InsertionLimited(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES,
                db.length);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCaseInsensitiveUsingSortInsertionLimited()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper =
            Arrays.asList(toUpper(field(projectRowType, 0)));
        Operator plan =
            project_DefaultTest(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES,
                    db.length),
                projectRowType,
                convertToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNonStringUsingSortInsertionLimited()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            sort_InsertionLimited(
                project,
                projectRowType,
                ordering,
                SortOption.SUPPRESS_DUPLICATES,
                db.length);
        Row[] expected = new Row[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMultipleColumnsUsingSortInsertionLimited()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_DefaultTest(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.SUPPRESS_DUPLICATES,
                    db.length),
                projectRowType,
                convertCaseInsensitiveToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCaseSensitiveUsingDistinctPartial()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        // Sort, preserving duplicates, so that we can test Distinct_Partial.
        Operator plan =
            distinct_Partial(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.PRESERVE_DUPLICATES,
                    db.length),
                projectRowType,
                Arrays.asList(caseSensitiveCollator));
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs"),
            row(projectRowType, "Aa_cs"),
            row(projectRowType, "BB_cs"),
            row(projectRowType, "Bb_cs"),
            row(projectRowType, "aA_cs"),
            row(projectRowType, "aa_cs"),
            row(projectRowType, "bB_cs"),
            row(projectRowType, "bb_cs"),
            row(projectRowType, "x"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testCaseInsensitiveUsingDistinctPartial()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 2)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseInsensitiveCollator);
        List<ExpressionGenerator> convertToUpper = Arrays.asList(toUpper(field(projectRowType, 0)));
        // Sort, preserving duplicates, so that we can test Distinct_Partial.
        Operator plan =
            project_DefaultTest(
                distinct_Partial(
                    sort_InsertionLimited(
                        project,
                        projectRowType,
                        ordering,
                        SortOption.PRESERVE_DUPLICATES,
                        db.length),
                    projectRowType,
                    Arrays.asList(caseInsensitiveCollator)),
                projectRowType,
                convertToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_CI"),
            row(projectRowType, "BB_CI"),
            row(projectRowType, "X"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNonStringUsingDistinctPartial()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(project.rowType(), 0), true);
        Operator plan =
            distinct_Partial(
                sort_InsertionLimited(
                    project,
                    projectRowType,
                    ordering,
                    SortOption.PRESERVE_DUPLICATES,
                    db.length),
                projectRowType,
                Arrays.asList((AkCollator)null));
        Row[] expected = new Row[] {
            row(projectRowType, 0L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMultipleColumnsUsingDistinctPartial()
    {
        Operator project = project_DefaultTest(
            groupScan_Default(group),
            tRowType,
            Arrays.asList(field(tRowType, 1),
                          field(tRowType, 2),
                          field(tRowType, 3)));
        RowType projectRowType = project.rowType();
        Ordering ordering = new Ordering();
        ordering.append(field(projectRowType, 0), true, caseSensitiveCollator);
        ordering.append(field(projectRowType, 1), true, caseInsensitiveCollator);
        ordering.append(field(projectRowType, 2), true);
        List<ExpressionGenerator> convertCaseInsensitiveToUpper =
            Arrays.asList(field(projectRowType, 0),
                          toUpper(field(projectRowType, 1)),
                          field(projectRowType, 2));
        Operator plan =
            project_DefaultTest(
                distinct_Partial(
                    sort_InsertionLimited(
                        project,
                        projectRowType,
                        ordering,
                        SortOption.PRESERVE_DUPLICATES,
                        db.length),
                    projectRowType,
                    Arrays.asList(caseSensitiveCollator, caseInsensitiveCollator, null)),
                projectRowType,
                convertCaseInsensitiveToUpper);
        Row[] expected = new Row[] {
            row(projectRowType, "AA_cs", "AA_CI", 0),
            row(projectRowType, "Aa_cs", "AA_CI", 0),
            row(projectRowType, "BB_cs", "BB_CI", 0),
            row(projectRowType, "Bb_cs", "BB_CI", 0),
            row(projectRowType, "aA_cs", "AA_CI", 0),
            row(projectRowType, "aa_cs", "AA_CI", 0),
            row(projectRowType, "bB_cs", "BB_CI", 0),
            row(projectRowType, "bb_cs", "BB_CI", 0),
            row(projectRowType, "x", "X", 0),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private int t;
    private TableRowType tRowType;
    private Group group;
    private AkCollator caseSensitiveCollator;
    private AkCollator caseInsensitiveCollator;
}
