/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server;

import com.akiban.server.error.PersistitAdapterException;
import com.persistit.Accumulator;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

/**
 * Mapping of indexes and types for the Accumulators used by the table status.
 * <p>
 * Note: Remember that <i>any</i> modification to existing values is an
 * <b>incompatible</b> data format change. It is only safe to stop using
 * an index position or add new ones at the end of the range.
 * </p>
 */
enum AccumInfo {
    ORDINAL(0, Accumulator.Type.SUM),
    ROW_COUNT(1, Accumulator.Type.SUM),
    UNIQUE_ID(2, Accumulator.Type.SEQ),
    AUTO_INC(3, Accumulator.Type.SUM),
    ;

    public Accumulator getAccumulator(Tree tree) {
        try {
            return tree.getAccumulator(getType(), getIndex());
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    AccumInfo(int index, Accumulator.Type type) {
        this.index = index;
        this.type = type;
    }

    private int getIndex() {
        return index;
    }

    private Accumulator.Type getType() {
        return type;
    }

    private final int index;
    private final Accumulator.Type type;
}
