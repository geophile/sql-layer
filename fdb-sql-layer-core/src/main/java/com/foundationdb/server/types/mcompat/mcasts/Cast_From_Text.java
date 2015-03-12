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
package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;

@SuppressWarnings("unused")
public final class Cast_From_Text {

    public static final TCast LONGTEXT_TO_MEDIUMTEXT = new TextCast(MString.LONGTEXT, MString.MEDIUMTEXT);
    public static final TCast LONGTEXT_TO_TEXT = new TextCast(MString.LONGTEXT, MString.TEXT);
    public static final TCast LONGTEXT_TO_TINYTEXT = new TextCast(MString.LONGTEXT, MString.TINYTEXT);
    public static final TCast LONGTEXT_TO_CHAR = new TextCast(MString.LONGTEXT, MString.CHAR);
    public static final TCast LONGTEXT_TO_VARCHAR = new TextCast(MString.LONGTEXT, MString.VARCHAR);

    public static final TCast MEDIUMTEXT_TO_LONGTEXT = new TextCast(MString.MEDIUMTEXT, MString.LONGTEXT);
    public static final TCast MEDIUMTEXT_TO_TEXT = new TextCast(MString.MEDIUMTEXT, MString.TEXT);
    public static final TCast MEDIUMTEXT_TO_TINYTEXT = new TextCast(MString.MEDIUMTEXT, MString.TINYTEXT);
    public static final TCast MEDIUMTEXT_TO_CHAR = new TextCast(MString.MEDIUMTEXT, MString.CHAR);
    public static final TCast MEDIUMTEXT_TO_VARCHAR = new TextCast(MString.MEDIUMTEXT, MString.VARCHAR);

    public static final TCast TEXT_TO_LONGTEXT = new TextCast(MString.TEXT, MString.LONGTEXT);
    public static final TCast TEXT_TO_MEDIUMTEXT = new TextCast(MString.TEXT, MString.MEDIUMTEXT);
    public static final TCast TEXT_TO_TINYTEXT = new TextCast(MString.TEXT, MString.TINYTEXT);
    public static final TCast TEXT_TO_CHAR = new TextCast(MString.TEXT, MString.CHAR);
    public static final TCast TEXT_TO_VARCHAR = new TextCast(MString.TEXT, MString.VARCHAR);

    public static final TCast TINYTEXT_TO_LONGTEXT = new TextCast(MString.TINYTEXT, MString.LONGTEXT);
    public static final TCast TINYTEXT_TO_MEDIUMTEXT = new TextCast(MString.TINYTEXT, MString.MEDIUMTEXT);
    public static final TCast TINYTEXT_TO_TEXT = new TextCast(MString.TINYTEXT, MString.TEXT);
    public static final TCast TINYTEXT_TO_CHAR = new TextCast(MString.TINYTEXT, MString.CHAR);
    public static final TCast TINYTEXT_TO_VARCHAR = new TextCast(MString.TINYTEXT, MString.VARCHAR);

    public static final TCast CHAR_TO_LONGTEXT = new TextCast(MString.CHAR, MString.LONGTEXT);
    public static final TCast CHAR_TO_MEDIUMTEXT = new TextCast(MString.CHAR, MString.MEDIUMTEXT);
    public static final TCast CHAR_TO_TEXT = new TextCast(MString.CHAR, MString.TEXT);
    public static final TCast CHAR_TO_TINYTEXT = new TextCast(MString.CHAR, MString.TINYTEXT);
    public static final TCast CHAR_TO_VARCHAR = new TextCast(MString.CHAR, MString.VARCHAR);

    public static final TCast VARCHAR_TO_LONGTEXT = new TextCast(MString.VARCHAR, MString.LONGTEXT);
    public static final TCast VARCHAR_TO_MEDIUMTEXT = new TextCast(MString.VARCHAR, MString.MEDIUMTEXT);
    public static final TCast VARCHAR_TO_TEXT = new TextCast(MString.VARCHAR, MString.TEXT);
    public static final TCast VARCHAR_TO_TINYTEXT = new TextCast(MString.VARCHAR, MString.TINYTEXT);
    public static final TCast VARCHAR_TO_CHAR = new TextCast(MString.VARCHAR, MString.CHAR);

    private static class TextCast extends TCastBase {
        private TextCast(TString sourceClass, TString targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            String in = source.getString();
            TInstance outputType = context.outputType();
            int maxLen = outputType.attribute(StringAttribute.MAX_LENGTH);
            String truncated = Strings.truncateIfNecessary(in, maxLen);
            if (in != truncated) {
                context.reportTruncate(in, truncated);
                in = truncated;
            }
            target.putString(in, TString.getCollator(outputType));
        }
    
        @Override
        public TInstance preferredTarget(TPreptimeValue source) {
            TInstance sourceType =  source.type();
            return targetClass().instance(sourceType.attribute(StringAttribute.MAX_LENGTH),
                    sourceType.attribute(StringAttribute.CHARSET),
                    sourceType.attribute(StringAttribute.COLLATION),
                    source.isNullable());
        }
    }

    private Cast_From_Text() {}
}
