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
package com.foundationdb.server.store.format.protobuf;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.CommonProtobuf.ProtobufRowFormat;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.ProtobufReadException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.foundationdb.server.store.format.protobuf.ProtobufStorageDescriptionHelper.*;

public class MemoryProtobufStorageDescription extends MemoryStorageDescription implements ProtobufStorageDescription
{
    private ProtobufRowFormat.Type formatType;
    private FileDescriptorProto fileProto;
    private transient ProtobufRowConverter rowConverter;

    public MemoryProtobufStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public MemoryProtobufStorageDescription(HasStorage forObject,
                                            MemoryProtobufStorageDescription other,
                                            String storageFormat) {
        super(forObject, other, storageFormat);
        this.formatType = other.formatType;
        this.fileProto = other.fileProto;
    }

    public synchronized ProtobufRowConverter ensureRowConverter() {
        if(rowConverter == null) {
            rowConverter = buildRowConverter(object, fileProto);
        }
        return rowConverter;
    }

    public void setFormatType(ProtobufRowFormat.Type formatType) {
        this.formatType = formatType;
    }

    public void readProtobuf(ProtobufRowFormat pbFormat) {
        formatType = pbFormat.getType();
        fileProto = pbFormat.getFileDescriptor();
    }

    //
    // StorageDescription
    //

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new MemoryProtobufStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        MemoryProtobufStorageDescription sd = new MemoryProtobufStorageDescription(forObject, storageFormat);
        sd.setFormatType(this.formatType);
        return sd;
    }


    @Override
    public void writeProtobuf(Storage.Builder builder) {
        super.writeProtobuf(builder);
        ProtobufStorageDescriptionHelper.writeProtobuf(builder, formatType, fileProto);
        writeUnknownFields(builder);
    }

    @Override
    public void validate(AISValidationOutput output) {
        super.validate(output);
        fileProto = validateAndGenerate(object, formatType, fileProto, output);
    }

    //
    // StoreStorageDescription
    //

    @Override
    public Row expandRow(MemoryStore store, Session session, MemoryStoreData storeData, Schema schema) {
        ensureRowConverter();
        DynamicMessage msg;
        try {
            msg = DynamicMessage.parseFrom(rowConverter.getMessageType(), storeData.rawValue);
        } catch(InvalidProtocolBufferException ex) {
            ProtobufReadException nex = new ProtobufReadException(rowConverter.getMessageType().getName(), ex.getMessage());
            nex.initCause(ex);
            throw nex;
        }
        return rowConverter.decode(msg);
    }

    @Override
    public void packRow(MemoryStore store, Session session, MemoryStoreData storeData, Row row) {
        ensureRowConverter();
        DynamicMessage msg = rowConverter.encode(row);
        storeData.rawValue = msg.toByteArray();
    }

    //
    // ProtobufStorageDescription
    //

    @Override
    public FileDescriptorProto getFileProto() {
        return fileProto;
    }

    @Override
    public ProtobufRowFormat.Type getFormatType() {
        return formatType;
    }
}
