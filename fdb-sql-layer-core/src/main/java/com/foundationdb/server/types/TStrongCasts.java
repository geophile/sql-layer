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
package com.foundationdb.server.types;

import com.foundationdb.server.error.AkibanInternalException;
import com.google.common.collect.ObjectArrays;

import java.util.HashSet;
import java.util.Set;

public abstract class TStrongCasts {

    public static TStrongCastsBuilder from(TClass firstSource, TClass... sources) {
        return new TStrongCastsBuilder(ObjectArrays.concat(sources, firstSource));
    }

    public abstract Iterable<TCastIdentifier> get();

    private TStrongCasts() { }

    public static class TStrongCastsBuilder {

        public TStrongCasts to(TClass firstTarget, TClass... targets) {
            return new StrongCastsGenerator(sources, ObjectArrays.concat(targets, firstTarget));
        }

        private TStrongCastsBuilder(TClass... sources) {
            assert sources.length > 0;
            this.sources = sources;
        }

        private final TClass[] sources;
    }

    private static class StrongCastsGenerator extends TStrongCasts {

        @Override
        public Iterable<TCastIdentifier> get() {
            Set<TCastIdentifier> results = new HashSet<>(targets.length * sources.length);
            for (TClass source :sources) {
                for (TClass target : targets) {
                    TCastIdentifier identifier = new TCastIdentifier(source, target);
                    if (!results.add(identifier))
                        throw new AkibanInternalException("duplicate strong cast identifier: " + identifier);
                }
            }
            return results;
        }

        private StrongCastsGenerator(TClass[] sources, TClass[] targets) {
            this.sources = sources;
            this.targets = targets;
        }

        private final TClass[] sources;
        private final TClass[] targets;
    }
}
