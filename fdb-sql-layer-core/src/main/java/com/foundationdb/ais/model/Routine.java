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
import com.foundationdb.server.error.InvalidRoutineException;

import java.util.*;

public class Routine 
{
    public static enum CallingConvention {
        JAVA, LOADABLE_PLAN, SQL_ROW, SCRIPT_FUNCTION_JAVA, SCRIPT_BINDINGS, 
        SCRIPT_FUNCTION_JSON, SCRIPT_BINDINGS_JSON, SCRIPT_LIBRARY
    }

    public static enum SQLAllowed {
        MODIFIES_SQL_DATA, READS_SQL_DATA, CONTAINS_SQL, NO_SQL
    }

    public static Routine create(AkibanInformationSchema ais, 
                                 String schemaName, String name,
                                 String language, CallingConvention callingConvention) {
        Routine routine = new Routine(ais, schemaName, name, language, callingConvention);
        ais.addRoutine(routine);
        return routine; 
    }
    
    protected Routine(AkibanInformationSchema ais,
                      String schemaName, String name,
                      String language, CallingConvention callingConvention) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "Routine", "schema name");
        AISInvariants.checkNullName(name, "Routine", "table name");
        AISInvariants.checkDuplicateRoutine(ais, schemaName, name);
        
        this.ais = ais;
        this.name = new TableName(schemaName, name);
        this.language = language;
        this.callingConvention = callingConvention;
    }
    
    public TableName getName() {
        return name;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public Parameter getNamedParameter(String name) {
        for (Parameter parameter : parameters) {
            if (name.equals(parameter.getName())) {
                return parameter;
            }
        }
        return null;
    }

    public boolean isProcedure() {
        return (returnValue == null);
    }

    public Parameter getReturnValue() {
        return returnValue;
    }

    public String getLanguage() {
        return language;
    }

    public CallingConvention getCallingConvention() {
        return callingConvention;
    }

    public SQLJJar getSQLJJar() {
        return sqljJar;
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }

    public String getExternalName() {
        if (methodName == null)
            return className;
        else if (className == null)
            return methodName;
        else
            return className + "." + methodName;
    }

    public String getDefinition() {
        return definition;
    }

    public SQLAllowed getSQLAllowed() {
        return sqlAllowed;
    }

    public int getDynamicResultSets() {
        return dynamicResultSets;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public boolean isCalledOnNullInput() {
        return calledOnNullInput;
    }

    protected void checkMutability() {
        ais.checkMutability();
    }
    
    public boolean isSystemRoutine() {
        if (name.getSchemaName().equalsIgnoreCase("sys") ||
                name.getSchemaName().equalsIgnoreCase("security_schema") ||
                name.getSchemaName().equalsIgnoreCase("sqlj")) {
            return true;
        }
        return false;
    }

    protected void addParameter(Parameter parameter)
    {
        checkMutability();
        switch (parameter.getDirection()) {
        case RETURN:
            returnValue = parameter;
            break;
        default:
            parameters.add(parameter);
        }
    }

    public void setExternalName(SQLJJar sqljJar, String className, String methodName) {
        checkMutability();
        switch (callingConvention) {
        case JAVA:
            AISInvariants.checkNullName(className, "Routine", "class name");
            AISInvariants.checkNullName(methodName, "Routine", "method name");
            break;
        case LOADABLE_PLAN:
            AISInvariants.checkNullName(className, "Routine", "class name");
            break;
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            AISInvariants.checkNullName(methodName, "Routine", "function name");
            break;
        default:
            throw new InvalidRoutineException(name.getSchemaName(), name.getTableName(), 
                                              "EXTERNAL NAME not allowed for " + callingConvention);
        }
        this.sqljJar = sqljJar;
        if (sqljJar != null)
            sqljJar.addRoutine(this);
        this.className = className;
        this.methodName = methodName;
    }

    public void setDefinition(String definition) {
        checkMutability();
        switch (callingConvention) {
        case JAVA:
        case LOADABLE_PLAN:
            throw new InvalidRoutineException(name.getSchemaName(), name.getTableName(), 
                                              "AS not allowed for " + callingConvention);
        }
        this.definition = definition;
    }

    public void setSQLAllowed(SQLAllowed sqlAllowed) {
        checkMutability();
        this.sqlAllowed = sqlAllowed;
    }

    public void setDynamicResultSets(int dynamicResultSets) {
        checkMutability();
        this.dynamicResultSets = dynamicResultSets;
    }

    public void setDeterministic(boolean deterministic) {
        checkMutability();
        this.deterministic = deterministic;
    }

    public void setCalledOnNullInput(boolean calledOnNullInput) {
        checkMutability();
        this.calledOnNullInput = calledOnNullInput;
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
    protected final List<Parameter> parameters = new ArrayList<>();
    protected Parameter returnValue = null;
    protected String language;
    protected CallingConvention callingConvention;
    protected SQLJJar sqljJar;
    protected String className, methodName;
    protected String definition;
    protected SQLAllowed sqlAllowed;
    protected int dynamicResultSets = 0;
    protected boolean deterministic, calledOnNullInput;
    protected long version;
}
