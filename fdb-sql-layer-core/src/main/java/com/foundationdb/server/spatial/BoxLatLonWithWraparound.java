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
import com.geophile.z.spatialobject.d2.Box;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.Region;
import com.geophile.z.space.RegionComparison;

import java.nio.ByteBuffer;

class BoxLatLonWithWraparound implements SpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s:%s, %s:%s)", left.yLo(), left.yHi(), right.xLo(), left.xHi());
    }

    // SpatialObject interface

    @Override
    public double[] arbitraryPoint()
    {
        return left.arbitraryPoint();
    }

    @Override
    public int maxZ()
    {
        return left.maxZ();
    }

    @Override
    public boolean containedBy(Region region)
    {
        // Only the topmost region can contain a box with wraparound
        return region.level() == 0;
    }

    @Override
    public boolean containedBy(Space space)
    {
        return left.containedBy(space) && right.containedBy(space);
    }

    @Override
    public RegionComparison compare(Region region)
    {
        RegionComparison cL = left.compare(region);
        RegionComparison cR = right.compare(region);
        if (cL == RegionComparison.REGION_INSIDE_OBJECT ||
            cR == RegionComparison.REGION_INSIDE_OBJECT) {
            return RegionComparison.REGION_INSIDE_OBJECT;
        } else if (cL == RegionComparison.REGION_OUTSIDE_OBJECT &&
                   cR == RegionComparison.REGION_OUTSIDE_OBJECT) {
            return RegionComparison.REGION_OUTSIDE_OBJECT;
        } else if (cL == RegionComparison.REGION_INSIDE_OBJECT &&
                   cR == RegionComparison.REGION_INSIDE_OBJECT) {
            assert false : region; // Can't be inside two disjoint boxes!
            return null;
        } else {
            return RegionComparison.REGION_OVERLAPS_OBJECT;
        }
    }

    @Override
    public void readFrom(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    // BoxLatLonWithWraparound interface

    public BoxLatLonWithWraparound(double latLo, double latHi, double lonLo, double lonHi)
    {
        left = new Box(latLo, latHi, Spatial.MIN_LON, lonHi);
        right = new Box(latLo, latHi, lonLo, Spatial.MAX_LON);
    }

    // Object state

    private final Box left;
    private final Box right;
}
