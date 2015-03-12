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
package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.protobuf.ProtobufDecompiler;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.NoSuchGroupException;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.store.format.protobuf.ProtobufStorageDescription;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;

import java.sql.Types;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Output group protobuf definition.
 */
public class GroupProtobufLoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                return new GroupProtobufCursor(context, bindings);
            }

            @Override
            public TransactionMode getTransactionMode() {
                return TransactionMode.READ_ONLY;
            }

            @Override
            public OutputMode getOutputMode() {
                return OutputMode.COPY_WITH_NEWLINE;
            }
        };
    }
    
    public static final int MESSAGES_PER_FLUSH = 100;

    public static class GroupProtobufCursor extends DirectObjectCursor {
        private final QueryContext context;
        private final QueryBindings bindings;
        private File tempFile;
        private BufferedReader reader;
        private int messagesSent;

        public GroupProtobufCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        public void open() {
            String currentSchema = context.getCurrentSchema();
            String schemaName, tableName;
            ValueSource value = valueNotNull(0);
            if (value == null)
                schemaName = currentSchema;
            else
                schemaName = value.getString();
            tableName = bindings.getValue(1).getString();
            TableName groupName = new TableName(schemaName, tableName);
            Group group = context.getAIS().getGroup(groupName);
            if (group == null)
                throw new NoSuchGroupException(groupName);
            StorageDescription storage = group.getStorageDescription();
            if (!(storage instanceof ProtobufStorageDescription))
                throw new InvalidParameterValueException("group does not use STORAGE_FORMAT protobuf");
            FileDescriptorProto fileProto = ((ProtobufStorageDescription)storage).getFileProto();
            try {
                tempFile = File.createTempFile("group", ".proto");
                try (FileWriter writer = new FileWriter(tempFile)) {
                    new ProtobufDecompiler(writer).decompile(fileProto);
                }
                reader = new BufferedReader(new FileReader(tempFile));
            }
            catch (IOException ex) {
                throw new AkibanInternalException("decompiling error", ex);
            }
            messagesSent = 0;
        }

        protected ValueSource valueNotNull(int index) {
            try {
                ValueSource value = bindings.getValue(index);
                if (value.isNull())
                    return null;
                else
                    return value;
            }
            catch (BindingNotSetException ex) {
                return null;
            }            
        }

        @Override
        public List<String> next() {
            if (messagesSent >= MESSAGES_PER_FLUSH) {
                messagesSent = 0;
                return Collections.emptyList();
            }
            String line;
            try {
                line = reader.readLine();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("temp file error", ex);
            }
            if (line == null)
                return null;
            else
                return Collections.singletonList(line);
        }

        @Override
        public void close() {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
                reader = null;
            }
            if (tempFile != null) {
                tempFile.delete();
                tempFile = null;
            }
        }
    }

    @Override
    public int[] jdbcTypes() {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
}
