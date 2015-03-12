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
package com.foundationdb.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** See src/main/resources/version/fdbsql_version.properties */
public class LayerVersionInfo
{
    private static final Logger LOG = LoggerFactory.getLogger(LayerVersionInfo.class);
    private static final String UNKNOWN_DEFAULT = "UNKNOWN";

    public LayerVersionInfo(Properties props) {
        this.version = getOrWarn(props, "version");
        this.versionShort = this.version.replace("-SNAPSHOT", "");

        String[] nums = versionShort.split("\\.");
        int major = -1, minor = -1, patch = -1, release = -1;
        try {
            major = Integer.parseInt(nums[0]);
            minor = Integer.parseInt(nums[1]);
            patch = Integer.parseInt(nums[2]);
            release = Integer.parseInt(getOrWarn(props, "release"));
        }
        catch (Exception e) {
            LOG.warn("Couldn't parse version number: " + versionShort);
        }
        this.versionMajor = major;
        this.versionMinor = minor;
        this.versionPatch = patch;
        this.versionRelease = release;
        this.versionIsSnapshot = !this.version.equals(this.versionShort);

        this.buildTime = getOrWarn(props, "build_time");

        this.gitBranch = getOrWarn(props, "git_branch");
        this.gitCommitTime = getOrWarn(props, "git_commit_time");
        this.gitDescribe = getOrWarn(props, "git_describe");
        this.gitHash = getOrWarn(props, "git_hash");
        this.gitHashShort = this.gitHash.substring(0, 7);

        this.versionLong = this.version + "+" + this.gitHashShort;
    }

    private static String getOrWarn(Properties props, String propName) {
        String value = props.getProperty(propName);
        if(value == null) {
            LOG.warn("Version property '{}' not found", propName);
            return UNKNOWN_DEFAULT;
        }
        return value;
    }


    /** As in pom: x.y.z[-SNAPSHOT] */
    public final String version;
    /** As in pom, no snapshot: x.y.z */
    public final String versionShort;
    /** As in pom, with short hash: x.y.z[-SNAPSHOT]+shortHash */
    public final String versionLong;

    public final int versionMajor;
    public final int versionMinor;
    public final int versionPatch;
    public final int versionRelease;
    public final boolean versionIsSnapshot;

    public final String buildTime;

    public final String gitBranch;
    public final String gitCommitTime;
    public final String gitDescribe;
    public final String gitHash;
    public final String gitHashShort;
}
