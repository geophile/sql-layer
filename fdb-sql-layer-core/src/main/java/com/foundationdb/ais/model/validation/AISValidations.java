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
package com.foundationdb.ais.model.validation;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class AISValidations {
    public static final AISValidation CHARACTER_SET_SUPPORTED = new CharacterSetSupported();
    public static final AISValidation COLLATION_SUPPORTED = new CollationSupported();
    public static final AISValidation COLUMN_POSITION_DENSE = new ColumnPositionDense();
    public static final AISValidation COLUMN_SIZES_MATCH = new ColumnMaxAndPrefixSizesMatch();
    public static final AISValidation GROUP_INDEX_DEPTH = new GroupIndexDepth();
    public static final AISValidation GROUP_INDEX_NOT_UNIQUE = new GroupIndexesNotUnique();
    public static final AISValidation GROUP_SINGLE_ROOT = new GroupSingleRoot();
    public static final AISValidation INDEX_COLUMN_IS_NOT_PARTIAL = new IndexColumnIsNotPartial();
    public static final AISValidation INDEX_HAS_COLUMNS = new IndexHasColumns();
    public static final AISValidation INDEX_IDS = new IndexIDValidation();
    public static final AISValidation JOIN_COLUMN_TYPES_MATCH = new JoinColumnTypesMatch();
    public static final AISValidation JOIN_TO_ONE_PARENT = new JoinToOneParent();
    public static final AISValidation JOIN_TO_PARENT_PK = new JoinToParentPK();
    public static final AISValidation VIRTUAL_TABLES_NOT_MIXED = new VirtualTablesNotMixed();
    public static final AISValidation VIRTUAL_TABLES_SINGLE = new VirtualTableSingleTableGroup();
    public static final AISValidation ORDINAL_ORDERING = new OrdinalOrdering();
    public static final AISValidation PRIMARY_KEY_IS_NOT_NULL = new PrimaryKeyIsNotNull();
    public static final AISValidation REFERENCES_CORRECT = new ReferencesCorrect();
    public static final AISValidation SEQUENCE_VALUES_VALID = new SequenceValuesValid();
    public static final AISValidation STORAGE_DESCRIPTIONS_VALID = new StorageDescriptionsValid();
    public static final AISValidation STORAGE_KEYS_UNIQUE = new StorageKeysUnique();
    public static final AISValidation SUPPORTED_COLUMN_TYPES = new SupportedColumnTypes();    
    public static final AISValidation TABLE_ID_UNIQUE = new TableIDsUnique();
    public static final AISValidation TABLES_IN_A_GROUP = new TablesInAGroup();
    public static final AISValidation TABLE_HAS_PRIMARY_KEY = new TableHasPrimaryKey();
    public static final AISValidation UUID_PRESENT = new UUIDPresent();
    public static final AISValidation VIEW_REFERENCES = new ViewReferences();
    public static final AISValidation FOREIGN_KEY_INDEXES = new ForeignKeyIndexes();
    public static final AISValidation TABLE_HAS_ONE_IDENTITY = new TableHasOneIdentityColumn();


    /** Validations any AIS should satisfy (e.g. references are valid) */
    public static final Collection<AISValidation> BASIC_VALIDATIONS = Collections.unmodifiableList(
        Arrays.asList(
            CHARACTER_SET_SUPPORTED,
            COLLATION_SUPPORTED,
            COLUMN_POSITION_DENSE,
            COLUMN_SIZES_MATCH,
            GROUP_INDEX_DEPTH,
            GROUP_INDEX_NOT_UNIQUE,
            GROUP_SINGLE_ROOT,
            INDEX_COLUMN_IS_NOT_PARTIAL,
            INDEX_HAS_COLUMNS,
            INDEX_IDS,
            JOIN_COLUMN_TYPES_MATCH,
            JOIN_TO_ONE_PARENT,
            JOIN_TO_PARENT_PK,
            VIRTUAL_TABLES_NOT_MIXED,
            VIRTUAL_TABLES_SINGLE,
            PRIMARY_KEY_IS_NOT_NULL,
            REFERENCES_CORRECT,
            SEQUENCE_VALUES_VALID,
            SUPPORTED_COLUMN_TYPES,
            TABLE_ID_UNIQUE,
            TABLES_IN_A_GROUP,
            TABLE_HAS_PRIMARY_KEY,
            TABLE_HAS_ONE_IDENTITY
            //VIEW_REFERENCES
        )
    );


    /** {@link #BASIC_VALIDATIONS} plus ones required by a running system (e.g. ordinals present) */
    public static final Collection<AISValidation> ALL_VALIDATIONS = Collections.unmodifiableList(
        combine(
            BASIC_VALIDATIONS,
            //
            ORDINAL_ORDERING,
            STORAGE_KEYS_UNIQUE,
            STORAGE_DESCRIPTIONS_VALID,
            UUID_PRESENT,
            FOREIGN_KEY_INDEXES
        )
    );


    private static List<AISValidation> combine(Collection<AISValidation> validations, AISValidation... extra) {
        List<AISValidation> combined = new ArrayList<>(validations.size() + extra.length);
        combined.addAll(validations);
        combined.addAll(Arrays.asList(extra));
        return combined;
    }

    static {
        // Single instances of validations so check that they have no state (see bug1078746).
        for(AISValidation v : ALL_VALIDATIONS) {
            for(Field f : v.getClass().getDeclaredFields()) {
                if((f.getModifiers() & Modifier.STATIC) == 0) {
                    throw new IllegalStateException("Field " + f.getName() + " of " + v.getClass().getName() + " is not static");
                }
            }
        }
    }

    private AISValidations()
    {}
}
