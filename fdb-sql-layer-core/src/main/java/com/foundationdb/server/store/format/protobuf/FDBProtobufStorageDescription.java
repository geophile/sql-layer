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
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.format.tuple.TupleStorageDescription;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.foundationdb.server.store.format.protobuf.ProtobufStorageDescriptionHelper.*;

public class FDBProtobufStorageDescription extends TupleStorageDescription implements ProtobufStorageDescription
{
    private ProtobufRowFormat.Type formatType;
    private FileDescriptorProto fileProto;
    private transient ProtobufRowConverter rowConverter;

    public FDBProtobufStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public FDBProtobufStorageDescription(HasStorage forObject, FDBProtobufStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        this.formatType = other.formatType;
        this.fileProto = other.fileProto;
    }

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new FDBProtobufStorageDescription(forObject, this, storageFormat);
    }
    
    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        FDBProtobufStorageDescription sd = new FDBProtobufStorageDescription(forObject, storageFormat);
        sd.setUsage(this.getUsage());
        sd.setFormatType(this.formatType);
        return sd;
    }

    @Override
    public FileDescriptorProto getFileProto() {
        return fileProto;
    }

    @Override
    public ProtobufRowFormat.Type getFormatType() {
        return formatType;
    }
    public void setFormatType(ProtobufRowFormat.Type formatType) {
        this.formatType = formatType;
    }

    public void readProtobuf(ProtobufRowFormat pbFormat) {
        formatType = pbFormat.getType();
        fileProto = pbFormat.getFileDescriptor();
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

    public synchronized ProtobufRowConverter ensureRowConverter() {
        if (rowConverter == null) {
            rowConverter = buildRowConverter(object, fileProto);
        }
        return rowConverter;
    }

    @Override
    public void packRow(FDBStore store, Session session,
                        FDBStoreData storeData, Row row) {
        ensureRowConverter();
        DynamicMessage msg = rowConverter.encode(row);
        storeData.rawValue = msg.toByteArray();
    }
    
    @Override 
    public Row expandRow (FDBStore store, Session session,
                            FDBStoreData storeData, Schema schema) {
        ensureRowConverter();
        DynamicMessage msg;
        try {
            msg = DynamicMessage.parseFrom(rowConverter.getMessageType(), storeData.rawValue);
        } catch (InvalidProtocolBufferException ex) {
            ProtobufReadException nex = new ProtobufReadException(rowConverter.getMessageType().getName(), ex.getMessage());
            nex.initCause(ex);
            throw nex;
        }
        Row row = rowConverter.decode(msg);
        row = overlayBlobData(row.rowType(), row, store, session);
        return row;
    }
}
