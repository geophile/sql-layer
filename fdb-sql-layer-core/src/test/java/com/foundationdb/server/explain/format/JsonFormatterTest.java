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
package com.foundationdb.server.explain.format;

import com.foundationdb.server.explain.*;

import org.junit.*;
import static org.junit.Assert.assertEquals;

public class JsonFormatterTest 
{
    @Test
    public void testDescribe_Explainer() {
        PrimitiveExplainer s = PrimitiveExplainer.getInstance("string");
        PrimitiveExplainer l = PrimitiveExplainer.getInstance(123);
        PrimitiveExplainer n = PrimitiveExplainer.getInstance(3.14);
        PrimitiveExplainer b = PrimitiveExplainer.getInstance(true);

        Attributes a = new Attributes();
        a.put(Label.NAME, PrimitiveExplainer.getInstance("TEST"));
        a.put(Label.OPERAND, s);
        a.put(Label.OPERAND, l);
        a.put(Label.OPERAND, n);
        a.put(Label.OPERAND, b);
        CompoundExplainer c = new CompoundExplainer(Type.FUNCTION, a);

        CompoundExplainer c2 = new CompoundExplainer(Type.EXTRA_INFO);
        c2.addAttribute(Label.COST, PrimitiveExplainer.getInstance("a lot"));
        c.addAttribute(Label.EXTRA_TAG, c2);

        String expected = 
            "{\n" +
            "  \"type\" : \"function\",\n" +
            "  \"operand\" : [ \"string\", 123, 3.14, true ],\n" +
            "  \"extra_tag\" : [ {\n" +
            "    \"type\" : \"extra_info\",\n" +
            "    \"cost\" : [ \"a lot\" ]\n" +
            "  } ],\n" +
            "  \"name\" : [ \"TEST\" ]\n" +
            "}";

        JsonFormatter f = new JsonFormatter();
        assertEquals(expected, f.format(c));
    }
}
