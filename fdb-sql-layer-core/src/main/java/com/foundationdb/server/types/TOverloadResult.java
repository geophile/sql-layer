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

import com.foundationdb.util.ArgumentValidation;

public class TOverloadResult {

    public static TOverloadResult fixed(TInstanceGenerator tInstanceGenerator) {
        // This is not the most efficient in that it requires an extra/pointless allocation, but it's only invoked
        // at startup.
        return new TOverloadResult(Category.FIXED, tInstanceGenerator.tClass(), tInstanceGenerator.attrs(), null, null);
    }

    public static TOverloadResult fixed(TClass tClass, int... attrs) {
        return new TOverloadResult(Category.FIXED, tClass, attrs, null, null);
    }

    public static TOverloadResult picking() {
        return new TOverloadResult(Category.PICKING, null, null, null, null);
    }

    public static TOverloadResult custom(TInstanceGenerator castSource, TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, null, rule, castSource);
    }

    public static TOverloadResult custom(TCustomOverloadResult rule) {
        return new TOverloadResult(Category.CUSTOM, null, null, rule, null);
    }

    public Category category() {
        return category;
    }

    public TInstance fixed(boolean nullable) {
        check(Category.FIXED);
        return fixedInstance.setNullable(nullable);
    }

    public TCustomOverloadResult customRule() {
        check(Category.CUSTOM);
        return customRule;
    }
    
    public TInstance customRuleCastSource(boolean nullable) {
        return castSource == null ? null : castSource.setNullable(nullable);
    }

    // object interface

    public String toString(boolean useShorthand) {
        switch (category) {
        case CUSTOM:    return "custom";
        case FIXED:     return fixedInstance.toString(useShorthand);
        case PICKING:   return "picking";
        default: throw new AssertionError(category);
        }
    }

    @Override
    public String toString() {
        return toString(false);
    }

    // private

    private void check(Category expected) {
        assert category == expected : "expected " + expected + " but was " + category;
    }

    // state

    private TOverloadResult(Category category,
                            TClass fixedTClass,
                            int[] fixedInstanceAttrs,
                            TCustomOverloadResult customRule,
                            TInstanceGenerator castSource)
    {
        this.category = category;
        this.fixedInstance = (fixedTClass == null) ? null : new TInstanceGenerator(fixedTClass, fixedInstanceAttrs);
        this.customRule = customRule;
        this.castSource = castSource;
        switch (category) {
        case CUSTOM:
            ArgumentValidation.notNull("custom rule", customRule);
            break;
        case FIXED:
            ArgumentValidation.notNull("fixed type", fixedTClass);
            ArgumentValidation.notNull("fixed type attributes", fixedInstanceAttrs);
            break;
        case PICKING:
            break;
        default:
            throw new AssertionError(category);
        }
    }

    private final Category category;
    private final TInstanceGenerator fixedInstance;
    private final TCustomOverloadResult customRule;
    private final TInstanceGenerator castSource;

    public enum Category {
        CUSTOM, FIXED, PICKING
    }
}
