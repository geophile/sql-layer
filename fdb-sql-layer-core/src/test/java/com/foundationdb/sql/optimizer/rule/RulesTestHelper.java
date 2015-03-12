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
package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.server.SchemaFactory;

import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;

public class RulesTestHelper
{
    private RulesTestHelper() {
    }

    public static List<BaseRule> loadRules(File file) throws Exception {
        Yaml yaml = new Yaml();
        FileInputStream istr = new FileInputStream(file);
        List list = yaml.loadAs(istr, List.class );
        istr.close();
        return parseRules(list);
    }

    public static List<BaseRule> parseRules(String str) throws Exception {
        Yaml yaml = new Yaml();
        List list = yaml.loadAs(str, List.class);
        return parseRules(list);
    }

    public static List<BaseRule> parseRules(List list) throws Exception {
        List<BaseRule> result = new ArrayList<>();
        for (Object obj : list) {
            if (obj instanceof String) {
                String cname = (String)obj;
                if (cname.indexOf('.') < 0)
                    cname = RulesTestHelper.class.getPackage().getName() + '.' + cname;
                result.add((BaseRule)Class.forName(cname).newInstance());
            }
            else {
                // TODO: Someday parse options from hash, etc.
                throw new Exception("Don't know what to do with " + obj);
            }
        }
        return result;
    }

    // Field associations needed to keep Index.getAllColumns() from getting NPE.
    public static void ensureFieldAssociations(AkibanInformationSchema ais) {
        new SchemaFactory().buildTableStatusAndFieldAssociations(ais);
    }
}
