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
package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstanceNormalizer;
import com.foundationdb.util.BitSets;

import java.util.ArrayList;
import java.util.List;

public final class TInputSetBuilder {

    public TInputSetBuilder covers(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), false, false, nextNormalizer));
        nextNormalizer = null;
        setExacts(covering);
        return this;
    }

    public TInputSetBuilder pickingCovers(TClass targetType, int... covering) {
        inputSets.add(new TInputSet(targetType, BitSets.of(covering), false, true, nextNormalizer));
        nextNormalizer = null;
        return this;
    }

    public TInputSetBuilder vararg(TClass targetType, int... covering) {
        assert vararg == null : vararg;
        vararg = new TInputSet(targetType, BitSets.of(covering), true, false, nextNormalizer);
        nextNormalizer = null;
        inputSets.add(vararg);
        exactsBuilder.setVararg(exact);
        return this;
    }

    public TInputSetBuilder pickingVararg(TClass targetType, int... covering) {
        assert vararg == null : vararg;
        vararg = new TInputSet(targetType, BitSets.of(covering), true, true, nextNormalizer);
        inputSets.add(vararg);
        nextNormalizer = null;
        exactsBuilder.setVararg(exact);
        return this;
    }

    public TInputSetBuilder reset(TInputSetBuilder builder) {
        inputSets = builder.toList();
        return this;
    }

    public TInputSetBuilder setExact(boolean exact) {
        this.exact = exact;
        return this;
    }

    public TInputSetBuilder nextInputPicksWith(TInstanceNormalizer nextNormalizer) {
        this.nextNormalizer = nextNormalizer;
        return this;
    }

    public void setExact(int pos, boolean exact) {
        if (exact) {
            exactsBuilder.set(pos, true);
        }
    }

    private void setExacts(int[] positions) {
        if (exact) {
            for (int pos : positions) {
                setExact(pos, true);
            }
        }
    }

    public InputSetFlags exactInputs() {
        return exactsBuilder.get();
    }

    public List<TInputSet> toList() {
        return new ArrayList<>(inputSets);
    }

    private final InputSetFlags.Builder exactsBuilder = new InputSetFlags.Builder();
    private TInstanceNormalizer nextNormalizer;
    private boolean exact = false;
    private TInputSet vararg = null;

    private List<TInputSet> inputSets = new ArrayList<>(4);
}
