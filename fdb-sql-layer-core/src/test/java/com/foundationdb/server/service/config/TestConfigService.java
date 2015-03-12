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
package com.foundationdb.server.service.config;

import com.foundationdb.server.error.ConfigurationPropertiesLoadException;

import com.foundationdb.server.service.text.FullTextIndexServiceImpl;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import com.google.inject.Inject;

public class TestConfigService extends ConfigurationServiceImpl {
    private final static File TESTDIR = new File("/tmp/fdb-sql-layer");
    private final static String TEST_FDB_ROOT_DIR = "sql_test";
    private static File dataDirectory = null;
    private static int dataDirectoryCounter = 0;
    private static volatile boolean doCleanOnUnload = false;
    private final Map<String, String> extraProperties;

    @Inject
    public TestConfigService() {
        this.extraProperties = getAndClearOverrides();
    }

    @Override
    public boolean testing()
    {
        return true;
    }

    @Override
    protected Map<String, String> loadProperties() {
        Map<String, String> ret = new HashMap<>(super.loadProperties());
        makeDataDirectory();
        ret.put(FullTextIndexServiceImpl.BACKGROUND_INTERVAL_PROPERTY, "1000");
        ret.put(FDB_ROOT_DIR_KEY, TEST_FDB_ROOT_DIR);
        ret.put(TEXT_INDEX_PATH_KEY, dataDirectory.getAbsolutePath());
        ret.put(BUCKET_COUNT_KEY, BUCKET_COUNT);
        ret.put(FEATURE_DDL_WITH_DML_KEY, "true");
        ret.put(FEATURE_SPATIAL_INDEX_KEY, "true");
        // extra = test overrides
        if (extraProperties != null) {
            for (final Map.Entry<String, String> property : extraProperties.entrySet()) {
                ret.put(property.getKey(), property.getValue());
            }
        }
        return ret;
    }


    @Override
    protected Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    public static File dataDirectory() {
        if (dataDirectory == null)
            makeDataDirectory();
        return dataDirectory;
    }

    public static File newDataDirectory() {
        dataDirectoryCounter++;
        makeDataDirectory();
        return dataDirectory;
    }

    private static void makeDataDirectory() {
        String name = "data";
        if (dataDirectoryCounter > 0)
            name += dataDirectoryCounter;
        dataDirectory = new File(TESTDIR, name);
        if (dataDirectory.exists()) {
            if (!dataDirectory.isDirectory()) {
                throw new ConfigurationPropertiesLoadException(dataDirectory.getName(), " it exists but isn't a directory");
            }
        } else {
            if (!dataDirectory.mkdirs()) {
                throw new ConfigurationPropertiesLoadException(dataDirectory.getName(), " it couldn't be created");
            }
            dataDirectory.deleteOnExit();
        }
    }

    public static void setOverrides(Map<String, String> startupConfigProperties) {
        if (!startupConfigPropertiesRef.compareAndSet(null, startupConfigProperties)) {
            throw new IllegalStateException("already set"); // sanity check; feel free to remove if it gets in your way
        }
    }

    private static Map<String, String> getAndClearOverrides() {
        return startupConfigPropertiesRef.getAndSet(null);
    }

    public static boolean getDoCleanOnUnload() {
        return doCleanOnUnload;
    }

    public static void setDoCleanOnUnload(boolean doClean) {
        doCleanOnUnload = doClean;
    }

    private static final AtomicReference<Map<String, String>> startupConfigPropertiesRef
            = new AtomicReference<>();
    public final static String FDB_ROOT_DIR_KEY = "fdbsql.fdb.root_directory";
    public final static String TEXT_INDEX_PATH_KEY = "fdbsql.text.indexpath";
    public final static String BUCKET_COUNT_KEY = "fdbsql.index_statistics.bucket_count";
    private final static String BUCKET_COUNT = "32";

    public final static String FEATURE_DDL_WITH_DML_KEY = "fdbsql.feature.ddl_with_dml_on";
    public final static String FEATURE_SPATIAL_INDEX_KEY = "fdbsql.feature.spatial_index_on";
}
