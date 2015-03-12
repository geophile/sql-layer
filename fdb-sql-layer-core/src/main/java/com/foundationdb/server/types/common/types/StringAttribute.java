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
package com.foundationdb.server.types.common.types;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.Serialization;
import com.foundationdb.server.types.texpressions.SerializeAs;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.CharacterTypeAttributes.CollationDerivation;

public enum StringAttribute implements Attribute
{
    /**
     * Number of characters
     * (Not byte length)
     */
    @SerializeAs(Serialization.LONG_1)MAX_LENGTH,

    @SerializeAs(Serialization.CHARSET) CHARSET,
    
    @SerializeAs(Serialization.COLLATION) COLLATION
    ;

    public static CharacterTypeAttributes characterTypeAttributes(TInstance type) {
        Object cacheRaw = type.getMetaData();
        if (cacheRaw != null) {
            return (CharacterTypeAttributes) cacheRaw;
        }
        CharacterTypeAttributes result;
        String charsetName = charsetName(type);
        int collationId = type.attribute(COLLATION);
        if (collationId == StringFactory.NULL_COLLATION_ID) {
            result = new CharacterTypeAttributes(charsetName, null, null);
        }
        else {
            // TODO add implicit-vs-explicit
            String collationName = AkCollatorFactory.getAkCollator(collationId).getScheme();
            CollationDerivation derivation = CollationDerivation.IMPLICIT;
            result = new CharacterTypeAttributes(charsetName, collationName, derivation);
        }
        type.setMetaData(result);
        return result;
    }

    public static String charsetName(TInstance type) {
        int charsetId = type.attribute(CHARSET);
        return StringFactory.Charset.of(charsetId);
    }

    public static TInstance copyWithCollation(TInstance type, CharacterTypeAttributes cattrs) {
        AkCollator collator = AkCollatorFactory.getAkCollator(cattrs.getCollation());
        return type.typeClass().instance(
                type.attribute(MAX_LENGTH),
                type.attribute(CHARSET), collator.getCollationId(),
                type.nullability());
    }
}
