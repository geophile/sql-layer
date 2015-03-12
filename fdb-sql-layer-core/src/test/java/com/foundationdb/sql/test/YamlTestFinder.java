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
package com.foundationdb.sql.test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.TimeZone;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;

public class YamlTestFinder
{
    static {
        String timezone = "UTC";
        DateTimeZone.setDefault(DateTimeZone.forID(timezone));
        // We still have usages of java.util.Date, but thanks to the ConfigurationServiceImpl.validateTimezone
        // These should always be the same in production, so leaving this here until we cleanup remaining usages
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    private static final String PROPERTY_PREFIX = "com.foundationdb.sql.test.yaml";

    /** The directory containing the YAML files. */
    private static final String RESOURCE_DIR_PROPERTY = PROPERTY_PREFIX + ".RESOURCE_DIR";

    /** Whether to search the resource directory recursively for test files. */
    private static final String RECURSIVE_PROPERTY = PROPERTY_PREFIX + ".RECURSIVE";

    /** A resource known to be in the root to look for and find where the rest are. */
    private static final String RESOURCE_MARKER = "/com/foundationdb/sql/test/yaml/README";

    /**
     * A regular expression matching the names of the YAML files in
     * the resource directories.
     */
    private static final String FILE_NAME_REGEXP = "test-.*[.]yaml"; 

    private final Collection<Object[]> params = new ArrayList<>();
    private final Pattern filenamePattern = Pattern.compile(FILE_NAME_REGEXP);
    private final String baseName;

    private YamlTestFinder(URL baseURL) {
        this.baseName = baseURL.toString();
    }
    
    private void addURL(URL url) {
        // URI.relativize() ought to do this, but is confused about jar:file: URLs.
        String name = url.toString();
        int idx;
        if (name.startsWith(baseName)) {
            idx = baseName.length();
        }
        else {
            idx = name.lastIndexOf('/');
            if (idx < 0) {
                idx = 0;
            }
            else {
                idx++;
            }
        }
        params.add(new Object[] {
                       name.substring(idx, name.length() - 5),
                       url
                   });
    }

    /** Return a collection of class instantiation parameters for YAML tests.
     * The parameters are: <code>String caseName, URL url</code>.
     */
    public static Iterable<Object[]> findTests() {
        YamlTestFinder finder;
        String resourceDirName = System.getProperty(RESOURCE_DIR_PROPERTY);
        if (resourceDirName != null) {
            // User-specified file location.
            File resourceDir = new File(resourceDirName);
            boolean recursive = Boolean.valueOf(System.getProperty(RECURSIVE_PROPERTY,
                                                                   "true"));
            try {
                finder = new YamlTestFinder(resourceDir.toURI().toURL());
            }
            catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            }
            finder.collectFiles(resourceDir, recursive);
        }
        else {
            URL url = YamlTestFinder.class.getResource(RESOURCE_MARKER);
            if (url == null) {
                throw new RuntimeException("Problem finding tests: " + RESOURCE_MARKER);
            }
            try {
                finder = new YamlTestFinder(new URL(url, "."));
            }
            catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            }
            if ("file".equals(url.getProtocol())) {
                // Maven-specified file location.
                finder.collectFiles(new File(url.getPath()).getParentFile(), true);
            }
            else {
                // Inside test.jar.
                finder.collectResources(url);
            }
        }
        return finder.params;
    }

    protected boolean match(String filename) {
        return filenamePattern.matcher(filename).matches();
    }

    /**
     * Add files from the directory that match the pattern to params, recursing
     * if appropriate.
     */
    private void collectFiles(File directory, final boolean recursive) {
        File[] files = directory.listFiles(
            new FileFilter() {
                @Override
                public boolean accept(File file) {
                    if (file.isDirectory()) {
                        if (recursive) {
                            collectFiles(file, recursive);
                        }
                    }
                    else {
                        if (match(file.getName())) {
                            try {
                                addURL(file.toURI().toURL());
                            }
                            catch (MalformedURLException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                    return false;
                }
            }
        );
        if (files == null) {
            throw new RuntimeException("Problem accessing directory: " + directory);
        }
    }

    // In normal operation, where mvn test is run from the sql-layer
    // root, Maven's reactor will substitute access to files in the
    // sibling module, which is much nicer for iterative development.
    // This is for the case where the test-jar is actually being used,
    // such as when mvn test is run in the fdb-sql-layer-core child.
    private void collectResources(URL url) {
        String fullURL = url.toString();
        int bang = fullURL.indexOf('!');
        if (!fullURL.startsWith("jar:file:") ||
            (bang < 0)) {
            throw new RuntimeException("Unexpected resource location: " + fullURL);
        }
        String jarFilename = fullURL.substring(9, bang);
        try {
            JarFile jarFile = new JarFile(jarFilename);
            Enumeration<JarEntry> e = jarFile.entries();
            while (e.hasMoreElements()) {
                JarEntry jarEntry = e.nextElement();
                if (jarEntry.isDirectory()) continue;
                String filename = jarEntry.getName();
                int idx = filename.lastIndexOf('/');
                String name = (idx < 0) ? filename : filename.substring(idx+1);
                if (match(name)) {
                    addURL(new URL("jar:file:" + jarFilename + "!/" + filename));
                }
            }
        }
        catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            throw new RuntimeException("Error reading from " + jarFilename, ex);
        }
    }

}