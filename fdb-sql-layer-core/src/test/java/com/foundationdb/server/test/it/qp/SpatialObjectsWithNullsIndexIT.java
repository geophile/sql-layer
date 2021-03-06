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
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.storeadapter.indexcursor.IndexCursorSpatial_InBox;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.spatial.Spatial;
import com.foundationdb.server.spatial.TestRecord;
import com.foundationdb.server.spatial.TreeIndex;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.WrappingByteSource;
import com.geophile.z.Pair;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.SpaceImpl;
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SpatialObjectsWithNullsIndexIT extends OperatorITBase
{
    @Override
    protected boolean doAutoTransaction()
    {
        return false;
    }

    @Override
    protected void setupCreateSchema()
    {
        table = createTable(
            "schema", "table",
            "id int not null",
            "lat decimal(10, 5)",
            "lon decimal(10, 5)",
            "wkb blob", // POINT($LAT $LON)
            "wkt text", // POINT($LAT $LON)
            "primary key(id)");
        createSpatialTableIndex("schema", "table", "idx_lat_lon", "GEO_LAT_LON", 0, 2, "lat", "lon");
        createSpatialTableIndex("schema", "table", "idx_wkt", "GEO_WKT", 0, 1, "wkt");
        createSpatialTableIndex("schema", "table", "idx_wkb", "GEO_WKB", 0, 1, "wkb");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        TableRowType tableRowType = schema.tableRowType(table(table));
        group = tableRowType.table().getGroup();
        latLonIndexRowType = indexType(table, "lat", "lon");
        wktIndexRowType = indexType(table, "wkt");
        wkbIndexRowType = indexType(table, "wkb");
        space = Spatial.createLatLonSpace();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected int lookaheadQuantum()
    {
        return 1;
    }

    @Test
    public void testLoad() throws ParseException
    {
        loadDBWithNulls();
        try (TransactionContext t = new TransactionContext()) {
            // Check table
            Operator plan = groupScan_Default(group);
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            try {
                cursor.openTopLevel();
                AbstractRow row;
                int id = 0;
                while ((row = (AbstractRow) cursor.next()) != null) {
                    JTSSpatialObject jtsSpatialObject = points.get(id);
                    Long latOrNull = lats.get(id);
                    Long lonOrNull = lons.get(id);
                    assertEquals(id, row.value(0).getInt32());
                    // lat/lon
                    if (nullLat(id)) {
                        assertTrue(row.value(1).isNull());
                    } else {
                        assertNotNull(latOrNull);
                        assertEquals(latOrNull, toDouble(row.value(1)), 0);
                    }
                    if (nullLon(id)) {
                        assertTrue(row.value(2).isNull());
                    } else {
                        assertNotNull(lonOrNull);
                        assertEquals(lonOrNull, toDouble(row.value(2)), 0);
                    }
                    // wkb, wkt
                    if (nullSpatialObject(id)) {
                        assertTrue(row.value(3).isNull());
                        assertTrue(row.value(4).isNull());
                    } else {
                        assertEquals(jtsSpatialObject, wkbToSpatialObject(row.value(3)));
                        assertEquals(jtsSpatialObject, wktToSpatialObject(row.value(4)));
                    }
                    id++;
                }
            } finally {
                cursor.closeTopLevel();
            }
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkt index
            Operator plan = indexScan_Default(wktIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            nullSpatialObject(id)
                            ? null
                            : new long[]{z, id};
                    }
                });
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check wkb index
            Operator plan = indexScan_Default(wkbIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            nullSpatialObject(id)
                            ? null
                            : new long[]{z, id};
                    }
                });
            compareRows(rows(wkbIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
        try (TransactionContext t = new TransactionContext()) {
            // Check lat/lon index
            Operator plan = indexScan_Default(latLonIndexRowType);
            long[][] expected = zToId.toArray(
                new ZToIdMapping.ExpectedRowCreator()
                {
                    @Override
                    public long[] fields(long z, int id)
                    {
                        return
                            nullLat(id) || nullLon(id)
                            ? null
                            : new long[]{z, id};
                    }
                });
            compareRows(rows(wktIndexRowType.physicalRowType(), sort(expected)),
                        cursor(plan, queryContext, queryBindings));
        }
    }

    @Test
    public void testSpatialQuery() throws IOException, InterruptedException
    {
        final int ID_COLUMN = 1;
        loadDBWithNulls();
        final int QUERIES = 100;
        for (int q = 0; q < QUERIES; q++) {
            try (TransactionContext t = new TransactionContext()) {
                JTSSpatialObject queryBox = randomBox();
                Set<Integer> expected;
                {
                    // Get the right answer
                    expected = new HashSet<>();
                    for (int id = 0; id < points.size(); id++) {
                        JTSSpatialObject jtsSpatialObject = points.get(id);
                        if (jtsSpatialObject != null && jtsSpatialObject.geometry().overlaps(queryBox.geometry())) {
                            expected.add(id);
                        }
                    }
                }
                {
                    // Get the expected access pattern
                    TestRecordFactory recordFactory = new TestRecordFactory();
                    // data
                    int id = 0;
                    TreeIndex dataIndex = new TreeIndex();
                    SpatialIndex<TestRecord> dataSpatialIndex = SpatialIndex.newSpatialIndex(SPACE, dataIndex);
                    for (double lat = Spatial.MIN_LAT; lat <= Spatial.MAX_LAT; lat += DELTA_LAT) {
                        for (double lon = Spatial.MIN_LON; lon <= Spatial.MAX_LON; lon += DELTA_LON) {
                            com.geophile.z.spatialobject.d2.Point point = new com.geophile.z.spatialobject.d2.Point(lat, lon);
                            dataSpatialIndex.add(point, recordFactory.initialize(point, id++));
                        }
                    }
                    // query
                    TreeIndex queryIndex = new TreeIndex();
                    SpatialIndex<TestRecord> querySpatialIndex = SpatialIndex.newSpatialIndex(SPACE, queryIndex);
                    Envelope envelope = queryBox.geometry().getEnvelopeInternal();
                    Box box = new Box(envelope.getMinX(),
                                      envelope.getMaxX(),
                                      envelope.getMinY(),
                                      envelope.getMaxY());
                    querySpatialIndex.add(box, recordFactory.initialize(box, 0), IndexCursorSpatial_InBox.MAX_Z);
                    // spatial join
                    SpatialJoin spatialJoin =
                        SpatialJoin.newSpatialJoin(SpatialJoin.Duplicates.INCLUDE);
                    Iterator<Pair<TestRecord, TestRecord>> iterator = spatialJoin.iterator(querySpatialIndex, dataSpatialIndex);
                    while (iterator.hasNext()) {
                        iterator.next();
                    }
                }
                { // WKT
                    // Get the query result using the wkt index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(wktIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(wktIndexRowType, boxBound);
                    Operator plan = indexScan_Default(wktIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                }
                { // WKB
                    // Get the query result using the wkb index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(wkbIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(wkbIndexRowType, boxBound);
                    Operator plan = indexScan_Default(wkbIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                }
                { // LAT/LON
                    // Get the query result using the lat/lon index
                    Set<Integer> actual = new HashSet<>();
                    IndexBound boxBound = new IndexBound(row(latLonIndexRowType, queryBox),
                                                         new SetColumnSelector(0));
                    IndexKeyRange box = IndexKeyRange.spatialObject(latLonIndexRowType, boxBound);
                    Operator plan = indexScan_Default(latLonIndexRowType, box, lookaheadQuantum());
                    Cursor cursor = API.cursor(plan, queryContext, queryBindings);
                    cursor.openTopLevel();
                    Row row;
                    while ((row = cursor.next()) != null) {
                        int id = getLong(row, ID_COLUMN).intValue();
                        actual.add(id);
                    }
                    // There should be no false negatives
                    assertTrue(actual.containsAll(expected));
                }
            }
        }
    }

    private void loadDBWithNulls()
    {
        try (TransactionContext t = new TransactionContext()) {
            int id = 0;
            for (long lat = LAT_LO; lat <= LAT_HI; lat += DELTA_LAT) {
                for (long lon = LON_LO; lon <= LON_HI; lon += DELTA_LON) {
                    Long latOrNull = nullLat(id) ? null : lat;
                    Long lonOrNull = nullLon(id) ? null : lon;
                    JTSSpatialObject point =
                        nullSpatialObject(id)
                        ? null
                        : point(lat, lon);
                    writeRow(session(), row(table, id, latOrNull, lonOrNull, point, point));
                    recordZToId(id, point);
                    lats.add(latOrNull);
                    lons.add(lonOrNull);
                    points.add(point);
                    id++;
                }
            }
            nIds = id;
        }
    }

    private static boolean nullLat(int id)
    {
        return id % 3 == 0;
    }

    private static boolean nullLon(int id)
    {
        return id % 5 == 0;
    }

    private static boolean nullSpatialObject(int id)
    {
        return nullLat(id) || nullLon(id);
    }

    private void recordZToId(int id, JTSSpatialObject box)
    {
        if (box != null) {
            long[] zs = new long[box.maxZ()];
            Spatial.shuffle(space, box, zs);
            for (int i = 0; i < zs.length && zs[i] != Space.Z_NULL; i++) {
                long z = zs[i];
                zToId.add(z, id);
            }
        }
    }

    private JTSSpatialObject randomBox()
    {
        double width = QUERY_WIDTH * random.nextDouble();
        double xLo = LAT_LO + (LAT_HI - LAT_LO - width) * random.nextDouble();
        double xHi = xLo + width;
        double height = QUERY_WIDTH * random.nextDouble();
        double yLo = LON_LO + (LON_HI - LON_LO - height) * random.nextDouble();
        double yHi = yLo + height;
        return box(xLo, xHi, yLo, yHi);
    }

    private JTSSpatialObject box(double xLo, double xHi, double yLo, double yHi)
    {
        Coordinate[] coords = new Coordinate[5];
        coords[0] = new Coordinate(xLo, yLo);
        coords[1] = new Coordinate(xLo, yHi);
        coords[2] = new Coordinate(xHi, yHi);
        coords[3] = new Coordinate(xHi, yLo);
        coords[4] = coords[0];
        return JTS.spatialObject(space, FACTORY.createPolygon(FACTORY.createLinearRing(coords), null));
    }

    private JTSSpatialObject point(double lat, double lon)
    {
        return JTS.spatialObject(space, FACTORY.createPoint(new Coordinate(lat, lon)));
    }

    private Row[] rows(RowType rowType, long[][] x)
    {
        Row[] rows = new Row[x.length];
        for (int i = 0; i < x.length; i++) {
            long[] a = x[i];
            Object[] oa = new Object[a.length];
            for (int j = 0; j < a.length; j++) {
                oa[j] = a[j];
            }
            rows[i] = row(rowType, oa);
        }
        return rows;
    }

    private long[][] sort(long[][] a)
    {
        Arrays.sort(a,
                    new Comparator<long[]>()
                    {
                        @Override
                        public int compare(long[] x, long[] y)
                        {
                            for (int i = 0; i < x.length; i++) {
                                if (x[i] < y[i]) {
                                    return -1;
                                }
                                if (x[i] > y[i]) {
                                    return 1;
                                }
                            }
                            return 0;
                        }
                    });
        return a;
    }

    private void dumpIndex(IndexRowType indexRowType)
    {
        Operator plan = indexScan_Default(indexRowType);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        try {
            cursor.openTopLevel();
            IndexRow row;
            while ((row = (IndexRow) cursor.next()) != null) {
                System.out.format("    %s\n", SpaceImpl.formatZ(row.z()));
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    private SpatialObject wkbToSpatialObject(ValueSource valueSource) throws ParseException
    {
        return Spatial.deserializeWKB(space, (byte[]) ValueSources.toObject(valueSource));
    }

    private SpatialObject wktToSpatialObject(ValueSource valueSource) throws ParseException
    {
        return Spatial.deserializeWKT(space, ValueSources.toStringSimple(valueSource));
    }

    private double toDouble(ValueSource valueSource)
    {
        BigDecimalWrapper bigDecimalWrapper = (BigDecimalWrapper) valueSource.getObject();
        return bigDecimalWrapper.asBigDecimal().doubleValue();
    }

    private static final Space SPACE = Spatial.createLatLonSpace();
    private static final int LAT_LO = (int) Spatial.MIN_LAT;
    private static final int LAT_HI = (int) Spatial.MAX_LAT;
    private static final int LON_LO = (int) Spatial.MIN_LON;
    private static final int LON_HI = (int) Spatial.MAX_LON;
    private static final int DELTA_LAT = 10;
    private static final int DELTA_LON = 10;
    private static final int QUERY_WIDTH = 30;
    private static final GeometryFactory FACTORY = new GeometryFactory();

    private int table;
    private Group group;
    private IndexRowType wktIndexRowType;
    private IndexRowType wkbIndexRowType;
    private IndexRowType latLonIndexRowType;
    private Space space;
    private ZToIdMapping zToId = new ZToIdMapping();
    List<JTSSpatialObject> points = new ArrayList<>();
    List<Long> lats = new ArrayList<>();
    List<Long> lons = new ArrayList<>();
    private int nIds;
    Random random = new Random(1234567);

    private static class TestRecordFactory implements Record.Factory<TestRecord>
    {
        @Override
        public TestRecord newRecord()
        {
            return new TestRecord(spatialObject, id);
        }

        public TestRecordFactory initialize(SpatialObject spatialObject, int id)
        {
            this.spatialObject = spatialObject;
            this.id = id;
            return this;
        }

        private SpatialObject spatialObject;
        private int id;
    }

    private static class SpatialJoinObserver extends SpatialJoin.InputObserver
    {
        @Override
        public void enter(long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.ENTER, z));
            // System.out.format("%s: ENTER %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void exit(long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.EXIT, z));
            // System.out.format("%s: EXIT  %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void randomAccess(com.geophile.z.Cursor cursor, long z)
        {
            events.add(new SpatialJoinEvent(operand, SpatialJoinEventType.RANDOM_ACCESS, z));
            // System.out.format("%s:     GOTO %s\n", operand, SpaceImpl.formatZ(z));
        }

        @Override
        public void sequentialAccess(com.geophile.z.Cursor cursor, long zRandomAccess, Record record)
        {
            events.add(new SpatialJoinEvent(operand,
                                            SpatialJoinEventType.SEQUENTIAL_ACCESS,
                                            record == null ? Space.Z_NULL : record.z()));
/*
            System.out.format("%s:     NEXT %s -> %s\n",
                              operand,
                              SpaceImpl.formatZ(zRandomAccess),
                              record == null ? null : SpaceImpl.formatZ(record.z()));
*/
        }

        SpatialJoinObserver(Operand operand, List<SpatialJoinEvent> events)
        {
            this.operand = operand;
            this.events = events;
        }

        private final Operand operand;
        private final List<SpatialJoinEvent> events;
    }

    enum Operand
    {
        QUERY, DATA
    }

    enum SpatialJoinEventType
    {
        RANDOM_ACCESS, SEQUENTIAL_ACCESS, ENTER, EXIT
    }

    private static class SpatialJoinEvent
    {
        @Override
        public String toString()
        {
            return String.format("%s: %s %s",
                                 operand == Operand.DATA ? "DATA " : "QUERY", SpaceImpl.formatZ(z), eventType);
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj.getClass() == this.getClass()) {
                SpatialJoinEvent that = (SpatialJoinEvent) obj;
                eq =
                    this.operand == that.operand &&
                    this.eventType == that.eventType &&
                    this.z == that.z;
            }
            return eq;
        }

        public SpatialJoinEvent(Operand operand, SpatialJoinEventType eventType, long z)
        {
            this.operand = operand;
            this.eventType = eventType;
            this.z = z;
        }

        private final Operand operand;
        private final SpatialJoinEventType eventType;
        private final long z;
    }}
