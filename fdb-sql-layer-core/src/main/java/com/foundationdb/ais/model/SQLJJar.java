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

import com.foundationdb.ais.model.validation.AISInvariants;

import java.net.URL;
import java.util.*;

public class SQLJJar
{
    public static SQLJJar create(AkibanInformationSchema ais, 
                                 String schemaName, String name,
                                 URL url) {
        SQLJJar sqljJar = new SQLJJar(ais, schemaName, name, url);
        ais.addSQLJJar(sqljJar);
        return sqljJar; 
    }
    
    protected SQLJJar(AkibanInformationSchema ais, 
                      String schemaName, String name,
                      URL url) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "SQJ/J jar", "schema name");
        AISInvariants.checkNullName(name, "SQJ/J jar", "jar name");
        AISInvariants.checkDuplicateSQLJJar(ais, schemaName, name);
        AISInvariants.checkNullField(url, "SQLJJar", "url", "URL");
        
        this.ais = ais;
        this.name = new TableName(schemaName, name);
        this.url = url;
    }
    
    public TableName getName() {
        return name;
    }

    public URL getURL() {
        return url;
    }

    public Collection<Routine> getRoutines() {
        return routines;
    }

    protected void checkMutability() {
        ais.checkMutability();
    }

    protected void addRoutine(Routine routine)
    {
        checkMutability();
        routines.add(routine);
    }

    public void removeRoutine(Routine routine) {
        routines.remove(routine);
    }

    public void setURL(URL url) {
        checkMutability();
        this.url = url;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected URL url;
    protected long version;
    protected transient final Collection<Routine> routines = new ArrayList<>();
}
