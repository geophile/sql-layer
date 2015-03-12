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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.types.TInstance;

import java.util.UUID;

public class Parameter
{
    public static enum Direction { IN, OUT, INOUT, RETURN };

    public static Parameter create(Routine routine, String name, Direction direction,
                                   TInstance type)
    {
        routine.checkMutability();
        if (name != null)
            AISInvariants.checkDuplicateParametersInRoutine(routine, name, direction);
        Parameter parameter = new Parameter(routine, name, direction, type);
        routine.addParameter(parameter);
        return parameter;
    }

    @Override
    public String toString()
    {
        StringBuffer str = new StringBuffer(direction.name());
        if (name != null)
            str.append(" ").append(name);
        str.append(" ").append(getTypeDescription());
        return str.toString();
    }

    public Routine getRoutine()
    {
        return routine;
    }

    public Direction getDirection()
    {
        return direction;
    }

    public String getName()
    {
        return name;
    }

    public TInstance getType() {
        return type;
    }

    public String getTypeName() {
        return type.typeClass().name().unqualifiedName();
    }

    public UUID getTypeBundleUUID() {
        return type.typeClass().name().bundleId().uuid();
    }

    public int getTypeVersion() {
        return type.typeClass().serializationVersion();
    }

    public String getTypeDescription()
    {
        return type.toStringConcise(true);
    }

    public Long getTypeParameter1()
    {
        return Column.getTypeParameter1(type);
    }

    public Long getTypeParameter2()
    {
        return Column.getTypeParameter2(type);
    }

    private Parameter(Routine routine,
                      String name,
                      Direction direction,
                      TInstance type)
    {
        this.routine = routine;
        this.name = name;
        this.direction = direction;
        this.type = type;
    }

    // State

    private final Routine routine;
    private final String name;
    private final Direction direction;
    private final TInstance type;
}
