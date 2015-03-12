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
package com.foundationdb.server.store;

import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.server.store.TableChanges.Change;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.TableChanges.IndexChange;
import com.foundationdb.util.ArgumentValidation;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;

public class ChangeSetHelper
{
    public static byte[] save(ChangeSet changeSet) {
        ArgumentValidation.notNull("changeSet", changeSet);
        checkFields(changeSet);
        int size = changeSet.getSerializedSize();
        byte[] buffer = new byte[size];
        CodedOutputStream stream = CodedOutputStream.newInstance(buffer);
        try {
            changeSet.writeTo(stream);
        } catch(IOException e) {
            // Only throws OutOfSpace, which shouldn't happen
            throw new IllegalStateException(e);
        }
        return buffer;
    }

    public static ChangeSet load(byte[] buffer) {
        ArgumentValidation.notNull("buffer", buffer);
        ChangeSet.Builder builder = ChangeSet.newBuilder();
        try {
            builder.mergeFrom(buffer);
        } catch(InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
        ChangeSet changeSet = builder.build();
        checkFields(changeSet);
        return changeSet;
    }

    public static Change createAddChange(String newName) {
        return Change.newBuilder().setChangeType(ChangeType.ADD.name()).setNewName(newName).build();
    }

    public static Change createDropChange(String oldName) {
        return Change.newBuilder().setChangeType(ChangeType.DROP.name()).setOldName(oldName).build();
    }

    public static Change createModifyChange(String oldName, String newName) {
        return Change.newBuilder().setChangeType(ChangeType.MODIFY.name()).setOldName(oldName).setNewName(newName).build();
    }

    public static Change createFromTableChange(TableChange tableChange) {
        switch(tableChange.getChangeType()) {
            case ADD:
                return ChangeSetHelper.createAddChange(tableChange.getNewName());
            case MODIFY:
                return ChangeSetHelper.createModifyChange(tableChange.getOldName(), tableChange.getNewName());
            case DROP:
                return ChangeSetHelper.createDropChange(tableChange.getOldName());
            default:
                throw new IllegalStateException(tableChange.getChangeType().toString());
        }
    }

    //
    // Internal
    //

    private static void checkFields(Change change) {
        requiredFields(change, Change.CHANGE_TYPE_FIELD_NUMBER);
        switch(ChangeType.valueOf(change.getChangeType())) {
            case ADD:
                requiredFields(change, Change.NEW_NAME_FIELD_NUMBER);
            break;
            case DROP:
                requiredFields(change, Change.OLD_NAME_FIELD_NUMBER);
            break;
            case MODIFY:
                requiredFields(change, Change.OLD_NAME_FIELD_NUMBER, Change.NEW_NAME_FIELD_NUMBER);
            break;
        }
    }

    private static void checkFields(IndexChange indexChange) {
        requiredFields(indexChange, IndexChange.INDEX_TYPE_FIELD_NUMBER, IndexChange.CHANGE_FIELD_NUMBER);
        checkFields(indexChange.getChange());
    }

    private static void checkFields(ChangeSet changeSet) {
        requiredFields(changeSet,
                       ChangeSet.CHANGE_LEVEL_FIELD_NUMBER,
                       ChangeSet.TABLE_ID_FIELD_NUMBER,
                       ChangeSet.OLD_SCHEMA_FIELD_NUMBER,
                       ChangeSet.OLD_NAME_FIELD_NUMBER);
        for(Change c : changeSet.getColumnChangeList()) {
            checkFields(c);
        }
        for(IndexChange c : changeSet.getIndexChangeList()) {
            checkFields(c);
        }
    }

    private static void requiredFields(Message msg, int... fields) {
        for(int fieldNumber : fields) {
            FieldDescriptor field = msg.getDescriptorForType().findFieldByNumber(fieldNumber);
            if(!msg.hasField(field)) {
                throw new IllegalArgumentException("Missing field: " + field.getName());
            }
        }
    }
}
