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
package com.foundationdb.server.types.service;

import com.foundationdb.server.error.UnsupportedColumnDataTypeException;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TName;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class TypesRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(TypesRegistry.class);

    static class BundleEntry {
        TBundleID bundleID;
        Map<String,TClass> typeClassByName =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        BundleEntry(TBundleID bundleID) {
            this.bundleID = bundleID;
        }
    }

    // NOTE: Lookup by UUID should be used for persistence of types.
    // Lookup by bundle name should only be used by tests that don't parse SQL DDL.
    private final Map<UUID,BundleEntry> bundlesByUUID = new HashMap<>();
    private final Map<String,BundleEntry> bundlesByName =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public TypesRegistry(Collection<? extends TClass> tClasses) {
        for (TClass tClass : tClasses) {
            TName tName = tClass.name();
            TBundleID bundleID = tName.bundleId();
            BundleEntry entry = bundlesByUUID.get(bundleID.uuid());
            if (entry == null) {
                entry = new BundleEntry(bundleID);
                bundlesByUUID.put(bundleID.uuid(), entry);
                BundleEntry oentry = bundlesByName.put(bundleID.name(), entry);
                if (oentry != null) {
                    // Tests may fail, but can work if only one used.
                    logger.warn("There is more than one bundle named {}: {} and {}",
                                new Object[] {
                                    bundleID.name(), bundleID.uuid(),
                                    oentry.bundleID.uuid()
                                });
                }
            }
            TClass prev = entry.typeClassByName.put(tName.unqualifiedName(), tClass);
            if (prev != null) {
                throw new IllegalStateException("Duplicate classes: " + prev +
                                                " and " + tClass);
            }
        }
    }

    public Collection<TBundleID> getTypeBundleIDs() {
        Collection<TBundleID> result = new ArrayList<>(bundlesByName.size());
        for (BundleEntry entry : bundlesByName.values())
            result.add(entry.bundleID);
        return result;
    }

    public Collection<TClass> getTypeClasses() {
        Collection<TClass> result = new ArrayList<>();
        for (BundleEntry entry : bundlesByName.values())
            result.addAll(entry.typeClassByName.values());
        return result;
    }

    public TClass getTypeClass(UUID bundleUUID, String name) {
        BundleEntry entry = bundlesByUUID.get(bundleUUID);
        if (entry == null)
            return null;
        else
            return entry.typeClassByName.get(name);
    }

    public TClass getTypeClass(String bundleName, String name) {
        BundleEntry entry = bundlesByName.get(bundleName);
        if (entry == null)
            return null;
        else
            return entry.typeClassByName.get(name);
    }

    /**
     * Get the type instance with the given name and parameters.
     * Table and column name are supplied for the sake of the error message.
     */
    public TInstance getType(UUID typeBundleUUID, String typeName, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeBundleUUID, typeName, typeVersion,
                       typeParameter1, typeParameter2, null, null,
                       nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(UUID typeBundleUUID, String typeName, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        return getType(typeBundleUUID, typeName, typeVersion,
                       typeParameter1, typeParameter2,
                       charset, collation, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(UUID typeBundleUUID, String typeName, int typeVersion,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             int defaultCharsetId, int defaultCollationId,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        TClass typeClass = getTypeClass(typeBundleUUID, typeName);
        if (typeClass == null) {
            throw new UnsupportedColumnDataTypeException(tableSchema, tableName, columnName,
                                                         typeName);
        }
        assert (typeVersion == typeClass.serializationVersion()) : typeClass;
        return getType(typeClass, typeParameter1, typeParameter2,
                       charset, collation, defaultCharsetId, defaultCollationId,
                       nullable, tableSchema, tableName, columnName);
    }

    /** For tests */
    public TInstance getType(String bundleName, String typeName,
                             Long typeParameter1, Long typeParameter2,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        TClass typeClass = getTypeClass(bundleName, typeName);
        if (typeClass == null) {
            throw new UnsupportedColumnDataTypeException(tableSchema, tableName, columnName,
                                                         typeName);
        }
        return getType(typeClass, typeParameter1, typeParameter2, null, null, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                       nullable, tableSchema, tableName, columnName);
    }

    public TInstance getType(String bundleName, String typeName,
                             Long typeParameter1, Long typeParameter2,
                             String charset, String collation,
                             boolean nullable,
                             String tableSchema, String tableName, String columnName) {
        TClass typeClass = getTypeClass(bundleName, typeName);
        if (typeClass == null) {
            throw new UnsupportedColumnDataTypeException(tableSchema, tableName, columnName,
                                                         typeName);
        }
        return getType(typeClass, typeParameter1, typeParameter2, charset, collation, StringFactory.DEFAULT_CHARSET_ID, StringFactory.DEFAULT_COLLATION_ID,
                       nullable, tableSchema, tableName, columnName);
    }

    protected TInstance getType(TClass typeClass,
                                Long typeParameter1, Long typeParameter2,
                                String charset, String collation,
                                int defaultCharsetId, int defaultCollationId,
                                boolean nullable,
                                String tableSchema, String tableName, String columnName) {
        if (typeClass.hasAttributes(StringAttribute.class)) {
            int charsetId = defaultCharsetId, collationId = defaultCollationId;
            if (charset != null) {
                charsetId = StringFactory.charsetNameToId(charset);
            }
            if (collation != null) {
                collationId = StringFactory.collationNameToId(collation);
            }
            return typeClass.instance((typeParameter1 == null) ? 0 : typeParameter1.intValue(),
                                   charsetId, collationId,
                                   nullable);
        }
        else if (typeParameter2 != null) {
            return typeClass.instance(typeParameter1.intValue(), typeParameter2.intValue(),
                                   nullable);
        }
        else if (typeParameter1 != null) {
            return typeClass.instance(typeParameter1.intValue(), nullable);
        }
        else {
            return typeClass.instance(nullable);
        }
    }

}
