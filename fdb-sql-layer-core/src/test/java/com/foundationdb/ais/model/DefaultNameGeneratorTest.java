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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DefaultNameGeneratorTest
{
    private DefaultNameGenerator generator = new DefaultNameGenerator();

    @Test
    public void identitySequenceName() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        TableName table = new TableName("test", "t");
        assertEquals(new TableName("test", "t_s1_seq"),
                     generator.generateIdentitySequenceName(ais, table, "s1"));
    }

    @Test
    public void identitySequenceNameConflict() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Sequence.create(ais, "test", "t_s_seq", 1, 1, 1, 10, false);
        assertEquals(new TableName("test", "t_s_seq$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", "t"), "s"));
    }

    @Test
    public void identitySequenceTruncate() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        char[] chars = new char[DefaultNameGenerator.MAX_IDENT];
        Arrays.fill(chars, 'a');
        String maxIdent = new String(chars);
        // Table too long
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), "s"));
        // Serial long
        assertEquals(new TableName("test", "t_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", "t"), maxIdent));
        // Both long
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), maxIdent));

        // Long with conflict
        Sequence.create(ais, "test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$1", 1, 1, 1, 10, false);
        assertEquals(new TableName("test", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$2"),
                     generator.generateIdentitySequenceName(ais, new TableName("test", maxIdent), "s"));
    }
}
