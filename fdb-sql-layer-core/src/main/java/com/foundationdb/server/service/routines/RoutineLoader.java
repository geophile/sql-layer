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
package com.foundationdb.server.service.routines;

import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Routine;

import com.foundationdb.qp.loadableplan.LoadablePlan;
import com.foundationdb.server.service.session.Session;
import java.lang.reflect.Method;
import java.io.IOException;
import java.util.jar.JarFile;

public interface RoutineLoader
{
    public ClassLoader loadSQLJJar(Session session, TableName jarName, boolean isSystemRoutine);
    public void checkUnloadSQLJJar(Session session, TableName jarName);
    public void registerSystemSQLJJar(SQLJJar sqljJar, ClassLoader classLoader);
    public JarFile openSQLJJarFile(Session session, TableName jarName) throws IOException;

    public LoadablePlan<?> loadLoadablePlan(Session session, TableName routineName);
    public Method loadJavaMethod(Session session, TableName routineName, long[] ret_aisGeneration);
    public boolean isScriptLanguage(Session session, String language);
    public ScriptPool<ScriptEvaluator> getScriptEvaluator(Session session, TableName routineName, long[] ret_aisGeneration);
    public ScriptPool<ScriptInvoker> getScriptInvoker(Session session, TableName routineName, long[] ret_aisGeneration);
    public ScriptPool<ScriptLibrary> getScriptLibrary(Session session, TableName routineName, long[] ret_aisGeneration);
    public void checkUnloadRoutine(Session session, TableName routineName);
}
