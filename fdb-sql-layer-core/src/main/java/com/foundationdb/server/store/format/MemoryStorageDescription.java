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
package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.validation.AISValidationFailure;
import com.foundationdb.ais.model.validation.AISValidationOutput;
import com.foundationdb.ais.protobuf.AISProtobuf.Storage;
import com.foundationdb.ais.protobuf.MemoryProtobuf;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.StorageDescriptionInvalidException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.StoreStorageDescription;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;

import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.WrappingByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;

/** Row<->byte[] via standard Java serialization. */
public class MemoryStorageDescription extends StoreStorageDescription<MemoryStore,MemoryStoreData>
{
    private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageDescription.class);

    private volatile UUID uuid;
    private volatile byte[] uuidBytes;

    public MemoryStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public MemoryStorageDescription(HasStorage forObject, MemoryStorageDescription other, String storageFormat) {
        super(forObject, other, storageFormat);
        setUUID(other.uuid);
    }

    public void setUUID(UUID uuid) {
        assert this.uuid == null;
        this.uuid = uuid;
        this.uuidBytes = (uuid == null) ? null : MemoryStore.packUUID(uuid);
    }

    public UUID getUUID() {
        return uuid;
    }

    public byte[] getUUIDBytes() {
        return uuidBytes;
    }

    //
    // StorageDescription
    //

    @Override
    public StorageDescription cloneForObject(HasStorage forObject) {
        return new MemoryStorageDescription(forObject, this, storageFormat);
    }

    @Override
    public StorageDescription cloneForObjectWithoutState(HasStorage forObject) {
        return new MemoryStorageDescription(forObject, storageFormat);
    }

    @Override
    public void writeProtobuf(Storage.Builder builder) {
        builder.setExtension(MemoryProtobuf.uuid, uuid.toString());
        writeUnknownFields(builder);
    }

    @Override
    public Object getUniqueKey() {
        return uuid;
    }

    @Override
    public String getNameString() {
        return uuid.toString();
    }

    @Override
    public void validate(AISValidationOutput output) {
        if(uuid == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "is missing UUID")));
        }
    }

    //
    // StoreStorageDescription
    //

    @Override
    public Row expandRow(MemoryStore store, Session session, MemoryStoreData storeData, Schema schema) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(storeData.rawValue);
            ObjectInputStream ois = new ObjectInputStream(bais);
            int tableID = ois.readInt();
            RowType rowType = schema.tableRowType(tableID);
            Object[] fields = new Object[rowType.nFields()];
            for(int i = 0; i < rowType.nFields(); ++i) {
                fields[i] = ois.readObject();
            }
            return new ValuesHolderRow(rowType, fields);
        } catch(ClassNotFoundException | IOException e) {
            LOG.error("Expand failure", e);
            throw new AkibanInternalException("Expand failure", e);
        }
    }

    @Override
    public void packRow(MemoryStore store, Session session, MemoryStoreData storeData, Row row) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            // Table ID
            oos.writeInt(row.rowType().table().getTableId());
            // Values
            for(int i = 0; i < row.rowType().nFields(); ++i) {
                ValueSource value = row.value(i);
                Object o = ValueSources.toObject(value);
                if(o instanceof WrappingByteSource) {
                    WrappingByteSource wbs = (WrappingByteSource)o;
                    byte[] bytes = wbs.byteArray();
                    if((wbs.byteArrayOffset() != 0) || (wbs.byteArrayLength() != bytes.length)) {
                        bytes = Arrays.copyOfRange(bytes, wbs.byteArrayOffset(), wbs.byteArrayLength());
                    }
                    o = bytes;
                }
                oos.writeObject(o);
            }
            storeData.rawValue = baos.toByteArray();
        } catch(IOException e) {
            LOG.error("Packing failure", e);
            throw new AkibanInternalException("Pack failure", e);
        }
    }
}
