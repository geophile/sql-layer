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
package com.foundationdb.server.service.servicemanager.configuration.yaml;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.util.Strings;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class YamlConfigurationTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> parameterizations() throws Exception {
        List<String> fileNames = Strings.dumpResource(YamlConfigurationTest.class, ".");
        ParameterizationBuilder builder = new ParameterizationBuilder();
        for (String fileName : fileNames) {
            if (fileName.startsWith(TEST_PREFIX)) {
                if (fileName.endsWith(TEST_SUFFIX)) {
                    builder.add(fileName, fileName);
                } else if (fileName.endsWith(DISABLED_SUFFIX)) {
                    builder.addFailing(fileName, fileName);
                }
            }
        }
        return builder.asList();
    }

    @Test
    public void compare() throws Exception {
        assert yamlFileName.endsWith(TEST_SUFFIX) : yamlFileName;

        String expectedFileName = yamlFileName.substring(0, yamlFileName.length() - TEST_SUFFIX.length())
                + EXPECTED_SUFFIX;

        List<String> expecteds = Strings.dumpResource(YamlConfigurationTest.class, expectedFileName);


        StringListConfigurationHandler stringsConfig = new StringListConfigurationHandler();
        InputStream testIS = YamlConfigurationTest.class.getResourceAsStream(yamlFileName);
        if (testIS == null) {
            throw new FileNotFoundException(yamlFileName);
        }
        int segment = 1;
        do {
            Reader testReader = new InputStreamReader(testIS, "UTF-8");
            YamlConfiguration yamlConfig = new YamlConfiguration(yamlFileName, testReader, getClass().getClassLoader());
            try {
                yamlConfig.loadInto(stringsConfig);
            } finally {
                testReader.close();
            }
            String nextFile = yamlFileName + '.' + (segment++);
            testIS = YamlConfigurationTest.class.getResourceAsStream(nextFile);
        } while (testIS != null);

        assertEquals("output", Strings.join(expecteds), Strings.join(stringsConfig.strings()));
    }

    public YamlConfigurationTest(String yamlFileName) {
        this.yamlFileName = yamlFileName;
    }

    // object state

    private final String yamlFileName;

    // class state

    private final static String TEST_PREFIX = "test-";
    private final static String TEST_SUFFIX = ".yaml";
    private final static String DISABLED_SUFFIX = ".disabled";
    private final static String EXPECTED_SUFFIX = ".expected";
}
