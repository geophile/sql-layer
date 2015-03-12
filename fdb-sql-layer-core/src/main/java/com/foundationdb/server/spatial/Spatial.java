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
package com.foundationdb.server.spatial;

import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/*

The lat/lon coordinate system is

- latitude: -90.0 to +90.0
- longitude: -180.0 to 180.0 with wraparound

The interleave pattern is [lon, lat, lon, lat, ..., lon], reflecting the fact that longitude covers a numeric range
twice that of latitude.

 */

public class Spatial
{
    public static Space createLatLonSpace()
    {
        int[] interleave = new int[LAT_BITS + LON_BITS];
        int dimension = 1; // Start with lon, as described above
        for (int d = 0; d < LAT_BITS + LON_BITS; d++) {
            interleave[d] = dimension;
            dimension = 1 - dimension;
        }
        return Space.newSpace(new double[]{MIN_LAT, MIN_LON},
                              new double[]{MAX_LAT, MAX_LON},
                              new int[]{LAT_BITS, LON_BITS},
                              interleave);
    }

    public static long shuffle(Space space, double x, double y)
    {
        com.geophile.z.spatialobject.d2.Point point = new com.geophile.z.spatialobject.d2.Point(x, y);
        long[] zValues = new long[1];
        space.decompose(point, zValues);
        long z = zValues[0];
        assert z != Space.Z_NULL;
        return z;
    }

    public static void shuffle(Space space, SpatialObject spatialObject, long[] zs)
    {
        space.decompose(spatialObject, zs);
    }

    public static byte[] serializeWKB(JTSSpatialObject spatialObject)
    {
        return io.get().wkbWriter().write(spatialObject.geometry());
    }

    public static SpatialObject deserializeWKB(Space space, byte[] bytes) throws ParseException
    {
        Geometry geometry = io.get().wkbReader().read(bytes);
        return
            geometry instanceof Point
            ? JTS.spatialObject(space, (Point) geometry)
            : JTS.spatialObject(space, geometry);
    }

    public static String serializeWKT(JTSSpatialObject spatialObject)
    {
        return io.get().wktWriter().write(spatialObject.geometry());
    }

    public static SpatialObject deserializeWKT(Space space, String string) throws ParseException
    {
        Geometry geometry = io.get().wktReader().read(string);
        return
            geometry instanceof Point
            ? JTS.spatialObject(space, (Point) geometry)
            : JTS.spatialObject(space, geometry);
    }

    public static final int LAT_LON_DIMENSIONS = 2;
    public static final double MIN_LAT = -90;
    public static final double MAX_LAT = 90;
    public static final double MIN_LON = -180;
    public static final double MAX_LON = 180;
    private static final int LAT_BITS = 28;
    private static final int LON_BITS = 29;
    private static final ThreadLocal<IO> io =
        new ThreadLocal<IO>()
        {
            @Override
            protected IO initialValue()
            {
                return new IO();
            }
        };
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Spatial.class);
    private static final Logger GEOPHILE_LOGGER = Logger.getLogger("");

    static
    {
        String geophileLogging = System.getProperty("geophile.logging");
        Level level;
        if (geophileLogging == null) {
            level = Level.OFF;
        } else {
            try {
                level = Level.parse(geophileLogging.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                level = Level.OFF;
                LOG.warn("Unsupported logging specification for geophile.logging: {}", geophileLogging);
            }
        }
        GEOPHILE_LOGGER.setLevel(level);
    }

    // Inner classes

    private static class IO
    {
        public WKBReader wkbReader()
        {
            if (wkbReader == null) {
                wkbReader = new WKBReader(factory);
            }
            return wkbReader;
        }

        public WKBWriter wkbWriter()
        {
            if (wkbWriter == null) {
                wkbWriter = new WKBWriter();
            }
            return wkbWriter;
        }

        public WKTReader wktReader()
        {
            if (wktReader == null) {
                wktReader = new WKTReader(factory);
            }
            return wktReader;
        }

        public WKTWriter wktWriter()
        {
            if (wktWriter == null) {
                wktWriter = new WKTWriter();
            }
            return wktWriter;
        }

        private final GeometryFactory factory = new GeometryFactory();
        private WKBReader wkbReader;
        private WKBWriter wkbWriter;
        private WKTReader wktReader;
        private WKTWriter wktWriter;
    }
}
