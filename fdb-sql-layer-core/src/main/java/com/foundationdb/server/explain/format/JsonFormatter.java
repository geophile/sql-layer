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

import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.Explainer;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Label;

import com.foundationdb.server.error.AkibanInternalException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

import static com.foundationdb.util.JsonUtils.createJsonGenerator;
import static com.foundationdb.util.JsonUtils.createPrettyJsonGenerator;
import static com.foundationdb.util.JsonUtils.makePretty;

public class JsonFormatter
{
    private final boolean pretty = true;

    public String format(Explainer explainer) {
        StringWriter str = new StringWriter();
        try {
            JsonGenerator generator = pretty ? createPrettyJsonGenerator(str) : createJsonGenerator(str);
            generate(generator, explainer);
            generator.flush();
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error writing to string", ex);
        }
        return str.toString();
    }

    public void format(Explainer explainer, Writer writer) throws IOException {
        format(explainer, createJsonGenerator(writer));
    }

    public void format(Explainer explainer, OutputStream stream) throws IOException {
        format(explainer, createJsonGenerator(stream, JsonEncoding.UTF8));
    }

    public void format(Explainer explainer, JsonGenerator generator) throws IOException {
        if (pretty) {
            makePretty(generator);
        }
        generate(generator, explainer);
        generator.flush();
    }

    public void generate(JsonGenerator generator, Explainer explainer) throws IOException {
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            generatePrimitive(generator, (PrimitiveExplainer)explainer);
            break;
        default:
            generateCompound(generator, (CompoundExplainer)explainer);
            break;
        }
    }

    protected void generatePrimitive(JsonGenerator generator, PrimitiveExplainer explainer) throws IOException {
        generator.writeObject(explainer.get());
    }

    protected void generateCompound(JsonGenerator generator, CompoundExplainer explainer) throws IOException {
        generator.writeStartObject();
        generator.writeObjectField("type", explainer.getType().name().toLowerCase());
        for (Map.Entry<Label, List<Explainer>> entry : explainer.get().entrySet()) {
            generator.writeFieldName(entry.getKey().name().toLowerCase());
            generator.writeStartArray();
            for (Explainer child : entry.getValue()) {
                generate(generator, child);
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
    }
}
