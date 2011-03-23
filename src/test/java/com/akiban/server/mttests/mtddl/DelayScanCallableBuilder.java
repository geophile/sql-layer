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

package com.akiban.server.mttests.mtddl;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.mttests.mtutil.TimePoints;

class DelayScanCallableBuilder {
    private final int tableId;
    private final int indexId;

    private boolean markFinish = true;
    private long initialDelay = 0;
    private DelayerFactory topOfLoopDelayer;
    private DelayerFactory beforeConversionDelayer;

    DelayScanCallableBuilder(int tableId, int indexId) {
        this.tableId = tableId;
        this.indexId = indexId;
    }

    DelayScanCallableBuilder topOfLoopDelayer(DelayerFactory delayer) {
        assert topOfLoopDelayer == null;
        topOfLoopDelayer = delayer;
        return this;
    }

    DelayScanCallableBuilder topOfLoopDelayer(final int beforeRow, final long delay, final String message) {
        return topOfLoopDelayer(singleDelayFactory(beforeRow, delay, message));
    }

    private DelayerFactory singleDelayFactory(final int beforeRow, final long delay, final String message) {
        return new DelayerFactory() {
            @Override
            public Delayer delayer(TimePoints timePoints) {
                long[] delays = new long[beforeRow+1];
                delays[beforeRow] = delay;
                return new Delayer(timePoints, delays).markBefore(beforeRow, message);
            }
        };
    }

    DelayScanCallableBuilder initialDelay(long delay) {
        this.initialDelay = delay;
        return this;
    }

    DelayScanCallableBuilder markFinish(boolean markFinish) {
        this.markFinish = markFinish;
        return this;
    }

    DelayScanCallableBuilder beforeConversionDelayer(DelayerFactory delayer) {
        assert beforeConversionDelayer == null;
        beforeConversionDelayer = delayer;
        return this;
    }

    DelayableScanCallable get(DDLFunctions ddl) {
        return new DelayableScanCallable(
                tableId, indexId, ddl,
                topOfLoopDelayer, beforeConversionDelayer,
                markFinish, initialDelay
        );
    }
}
