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
package com.foundationdb.server.test.costmodel;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;

import org.junit.Test;

import java.util.*;

import static com.foundationdb.qp.operator.API.*;



public class Select_HashTableCT extends CostModelBase
{
    static int ROW_BINDING_POSITION = 100;
    static int TABLE_BINDING_POSITION = 200;

    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        drivingRowTypes = new TableRowType[] { d1RowType,d2RowType,d3RowType,d4RowType,d5RowType};
        hashedRowTypes = new TableRowType[] { h1RowType,h2RowType,h3RowType,h4RowType,h5RowType};
        drivingTableIDs = new int[] {d1,d2,d3,d4,d5};
        hashedTableIDs = new int[] {h1,h2,h3,h4,h5};
        // Load-only
        run(MEASURED_RUNS, true, true, 2, 1);
        System.out.println("********************LOADING TESTS********************");
        for(int columns = 2; columns <= 6; columns++) {
            for (int joins = 1; joins < columns; joins++) {
                run(MEASURED_RUNS, true, true, columns, joins);
            }
        }
        //Load and Join
        run(MEASURED_RUNS, false, true, 2, 1);
        System.out.println("********************FULL TESTS********************");
        for(int columns = 2; columns <= 6; columns++) {
            for (int joins = 1; joins < columns; joins++) {
                System.gc();
                run(MEASURED_RUNS, false, true, columns, joins);
            }
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        // Schema is similar to that in Select_BloomHashTableIT
        String schemaName = schemaName();
        String d1TableName = newTableName();
        String d2TableName = newTableName();
        String d3TableName = newTableName();
        String d4TableName = newTableName();
        String d5TableName = newTableName();
        String h1TableName = newTableName();
        String h2TableName = newTableName();
        String h3TableName = newTableName();
        String h4TableName = newTableName();
        String h5TableName = newTableName();
        String emptyTable = newTableName();

        d1 = createTable(
                schemaName, d1TableName,
                "x int");
        h1 = createTable(
                schemaName, h1TableName,
                "x int");
        d2 = createTable(
                schemaName, d2TableName,
                "x int", "y int");
        h2 = createTable(
                schemaName, h2TableName,
                "x int", "y int");
        d3 = createTable(
                schemaName, d3TableName,
                "x int", "y int", "z int");
        h3 = createTable(
                schemaName, h3TableName,
                "x int", "y int", "z int");
        d4 = createTable(
                schemaName, d4TableName,
                "x int", "y int", "z int", "a int");
        h4 = createTable(
                schemaName, h4TableName,
                "x int", "y int", "z int", "a int");
        d5 = createTable(
                schemaName, d5TableName,
                "x int", "y int", "z int", "a int", "b int");
        h5 = createTable(
                schemaName, h5TableName,
                "x int", "y int", "z int", "a int", "b int");
        emptyTableID = createTable(
                schemaName, emptyTable,
                "x int", "y int", "z int", "a int", "b int");

        schema = SchemaCache.globalSchema(ais());
        d1RowType = schema.tableRowType(table(d1));
        h1RowType = schema.tableRowType(table(h1));
        d2RowType = schema.tableRowType(table(d2));
        h2RowType = schema.tableRowType(table(h2));
        d3RowType = schema.tableRowType(table(d3));
        h3RowType = schema.tableRowType(table(h3));
        d4RowType = schema.tableRowType(table(d4));
        h4RowType = schema.tableRowType(table(h4));
        d5RowType = schema.tableRowType(table(d5));
        h5RowType = schema.tableRowType(table(h5));
        emptyTableRowType = schema.tableRowType(table(emptyTableID));



        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }
/** x+=2 gives it a 50% match rate **/
    protected void populateDB()
    {
        int multiples[] = {53,97,193,389,769};
        for (int x = 0; x < HASHED_ROWS[0]/2; x++) {
            writeRow(h1, x, x * multiples[0]); // x, hidden_pk
            writeRow(h1, x, x * multiples[0]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[0]; x +=2) {
            writeRow(d1, x, x * multiples[0]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[1]/2; x++) {
            writeRow(h2, x, x * multiples[0], x * multiples[1]); // x, hidden_pk
            writeRow(h2, x, x * multiples[0], x * multiples[1]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[1]; x +=2) {
            writeRow(d2, x, x * multiples[0], x * multiples[1]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[2]/2; x++) {
            writeRow(h3, x, x * multiples[0], x * multiples[1], x * multiples[2]); // x, hidden_pk
            writeRow(h3, x, x * multiples[0], x * multiples[1], x * multiples[2]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[2]; x +=2) {
            writeRow(d3, x, x * multiples[0], x * multiples[1], x * multiples[2]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[3]/2; x++) {
            writeRow(h4, x, x * multiples[0], x * multiples[1], x * multiples[2], x * multiples[3]); // x, hidden_pk
            writeRow(h4, x, x * multiples[0], x * multiples[1], x * multiples[2], x * multiples[3]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[3]; x +=2) {
            writeRow(d4, x, x * multiples[0], x * multiples[1], x * multiples[2], x * multiples[3]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[4] /2; x++) {
            writeRow(h5,
                                          x,
                                          x * multiples[0],
                                          x * multiples[1],
                                          x * multiples[2],
                                          x * multiples[3],
                                          x * multiples[4]); // x, hidden_pk
            writeRow(h5,
                                          x,
                                          x * multiples[0],
                                          x * multiples[1],
                                          x * multiples[2],
                                          x * multiples[3],
                                          x * multiples[4]); // x, hidden_pk
        }
        for (int x = 0; x < HASHED_ROWS[4] * 2; x += 2 ){
            writeRow(d5,
                                          x,
                                          x * multiples[0],
                                          x * multiples[1],
                                          x * multiples[2],
                                          x * multiples[3],
                                          x * multiples[4]); // x, hidden_pk

        }

    }

    private void run(int runs, boolean loadOnly, boolean report, int columnCount, int joinColumns) {

        Operator plan = loadOnly ? planLoadOnly(columnCount, joinColumns) : planLoadAndSelect(columnCount, joinColumns);
        Row row;
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {

            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            while ((row = cursor.next()) != null) {
            }

        }
        long stop = System.nanoTime();
        long planNsec = stop - start;
        if (loadOnly) {
            planNsec -= (innerTimeOperator.elapsedNsec() + outerTimeOperator.elapsedNsec());
            double averageUsecPerRow = planNsec / (1000.0 * runs * HASHED_ROWS[columnCount - 2]);
            if (report) {
                System.out.println(String.format("\nload only: joined on %d out of %d columns: %2.5s usec/row", joinColumns, columnCount, averageUsecPerRow));
            }
        } else {
            planNsec -= (innerTimeOperator.elapsedNsec() + outerTimeOperator.elapsedNsec());
            double averageUsecPerRow = planNsec / (1000.0 * runs * HASHED_ROWS[columnCount - 2]);
            if (report) {
                System.out.println(String.format("\nload & join: joined on %d out of %d columns: %2.5s usec/row", joinColumns, columnCount, averageUsecPerRow));
            }
        }
    }

    public Operator planLoadOnly(int columnCount, int joinColumns)
    {
        RowType innerRowType = hashedRowTypes[columnCount -2];
        RowType outerRowType = emptyTableRowType;
        // filterInput loads the filter with F rows containing the given testId.
        Operator innerStream = new TimeOperator(groupScan_Default(innerRowType.table().getGroup()));

        Operator outerStream = groupScan_Default(outerRowType.table().getGroup());

        // For the  index scan retrieving rows from the F(x) index given a D index row

        // Use a bloom filter loaded by filterInput. Then for each input row, check the filter (projecting
        // D rows on (x)), and, for positives, check F using an index scan keyed by D.x.
        int joinFields[] = new int[joinColumns];
        for(int i = 0;  i <joinColumns; i++) {
            joinFields[i] = i;
        }

        return hashJoinPlan(outerRowType, innerRowType,outerStream,innerStream, joinFields, joinFields);
    }

    public Operator planLoadAndSelect(int columnCount, int joinColumns) {
        RowType innerRowType = hashedRowTypes[columnCount - 2];
        RowType outerRowType = drivingRowTypes[columnCount -2];
        // filterInput loads the filter with F rows containing the given testId.
        Operator innerStream = groupScan_Default(innerRowType.table().getGroup());

        Operator outerStream = groupScan_Default(outerRowType.table().getGroup());
        int joinFields[] = new int[joinColumns];
        for(int i = 0;  i <joinColumns; i++) {
            joinFields[i] = i;
        }
        return hashJoinPlan(outerRowType, innerRowType, outerStream, innerStream, joinFields, joinFields);
    }


        private Operator hashJoinPlan( RowType outerRowType,
                                   RowType innerRowType,
                                   Operator outerStream,
                                   Operator innerStream,
                                   int outerJoinFields[],
                                   int innerJoinFields[]) {

        List<TPreparedExpression> expressions = new ArrayList<>();
        for( int i = 0; i < outerRowType.nFields(); i++){
            expressions.add(new TPreparedBoundField(outerRowType, ROW_BINDING_POSITION, i));
        }
        for( int i = 0, j = 0; i < innerRowType.nFields(); i++){
            if(j < innerJoinFields.length && innerJoinFields[j] == i) {
                j++;
            }else{
                expressions.add(new TPreparedField(innerRowType.typeAt(i), i));
            }
        }

        List<TPreparedExpression> outerExpressions = new ArrayList<>();
        for (int i : outerJoinFields){
            outerExpressions.add(new TPreparedBoundField(outerRowType, ROW_BINDING_POSITION, i));
        }
        List<TPreparedExpression> innerExpressions = new ArrayList<>();
        for(int i : innerJoinFields){
            innerExpressions.add(new TPreparedField(innerRowType.typeAt(i), i));
        }
        innerTimeOperator = new TimeOperator(innerStream);
        outerTimeOperator = new TimeOperator(outerStream);
        Operator project = project_Default(
                hashTableLookup_Default(
                        innerRowType,
                        outerExpressions,
                        TABLE_BINDING_POSITION
                ),
                innerRowType,
                expressions
        );

         return using_HashTable(
                innerTimeOperator,
                innerRowType,
                innerExpressions,
                TABLE_BINDING_POSITION++,
                map_NestedLoops(
                        outerTimeOperator,
                        project,
                       ROW_BINDING_POSITION++,
                        false,
                        1
                ),
                null, null
            );
}



    private static final int WARMUP_RUNS = 100;
    private static final int MEASURED_RUNS = 80;

    private int d1, d2, d3, d4, d5;
    private int h1, h2, h3, h4, h5;
    //private static final long  HASHED_ROWS[] = { 2000,1500,1200,1000, 800};
    private static final long  HASHED_ROWS[] = { 2000,1750,1500,1250, 1000};

    private int emptyTableID;
    
    private TableRowType d1RowType;
    private TableRowType d2RowType;
    private TableRowType d3RowType;
    private TableRowType d4RowType;
    private TableRowType d5RowType;
    private TableRowType h1RowType;
    private TableRowType h2RowType;
    private TableRowType h3RowType;
    private TableRowType h4RowType;
    private TableRowType h5RowType;
    private TableRowType emptyTableRowType;

    int drivingTableIDs[];
    int hashedTableIDs[];
    TableRowType drivingRowTypes[];
    TableRowType hashedRowTypes[];

    private TimeOperator innerTimeOperator;
    private TimeOperator outerTimeOperator;

}
