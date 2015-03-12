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
package com.foundationdb.server.service.text;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.Strings;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

/** Given <code>Row</code>s in hkey order, create <code>Document</code>s. */
public class RowIndexer implements Closeable
{
    private Map<RowType,Integer> ancestorRowTypes;
    private Row[] ancestors;
    private Set<RowType> descendantRowTypes;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private IndexWriter writer;
    private Document currentDocument;
    private long documentCount;
    private String keyEncodedString;
    private boolean updating;

    private static final Logger logger = LoggerFactory.getLogger(RowIndexer.class);

    public RowIndexer(FullTextIndexInfo index, IndexWriter writer, boolean updating) {
        TableRowType indexedRowType = index.getIndexedRowType();
        int depth = indexedRowType.table().getDepth();
        ancestorRowTypes = new HashMap<>(depth+1);
        ancestors = new Row[depth+1];
        fieldsByRowType = index.getFieldsByRowType();
        Set<RowType> rowTypes = index.getRowTypes();
        descendantRowTypes = new HashSet<>(rowTypes.size() - ancestorRowTypes.size());
        for (RowType rowType : rowTypes) {
            if ((rowType == indexedRowType) ||
                rowType.ancestorOf(indexedRowType)) {
                Integer ancestorDepth = rowType.table().getDepth();
                ancestorRowTypes.put(rowType, ancestorDepth);
            }
            else if (indexedRowType.ancestorOf(rowType)) {
                descendantRowTypes.add(rowType);
            }
            else {
                assert false : "Not ancestor or descendant " + rowType;
            }
        }
        this.writer = writer;
        this.updating = updating;
        currentDocument = null;
    }

    public void indexRow(Row row) throws IOException {
        if (row == null) {
            addDocument();
            return;
        }
        RowType rowType = row.rowType();
        Integer ancestorDepth = ancestorRowTypes.get(rowType);
        if (ancestorDepth != null) {
            ancestors[ancestorDepth] = row;
            if (ancestorDepth == ancestors.length - 1) {
                addDocument();
                currentDocument = new Document();
                getKeyBytes(row);
                addFields(row, fieldsByRowType.get(rowType));
                for (int i = 0; i < ancestors.length - 1; i++) {
                    Row ancestor = ancestors[i];
                    if (ancestor != null) {
                        // We may have remembered an ancestor with no
                        // children and then this row is an orphan.
                        if (ancestor.ancestorOf(row)) {
                            addFields(ancestor, fieldsByRowType.get(ancestor.rowType()));
                        }
                        else {
                            ancestors[i] = null;
                        }
                    }
                }
            }
        }
        else if (descendantRowTypes.contains(rowType)) {
            Row ancestor = ancestors[ancestors.length - 1];
            if ((ancestor != null) && ancestor.ancestorOf(row)) {
                addFields(row, fieldsByRowType.get(rowType));
            }
        }
    }
    
    public long indexRows(Cursor cursor) throws IOException {
        documentCount = 0;
        cursor.openTopLevel();
        Row row;
        do {
            row = cursor.next();
            indexRow(row);
        } while (row != null);
        cursor.closeTopLevel();
        return documentCount;
    }

    protected void updateDocument(Cursor cursor, byte hkeyBytes[]) throws IOException
    {
        if (indexRows(cursor) == 0)
        {
            String encoded = encodeBytes(hkeyBytes, 0, hkeyBytes.length);
            writer.deleteDocuments(new Term(IndexedField.KEY_FIELD, encoded));
            logger.debug("Deleted documents with encoded byptes: " + encoded);
        }
    }

    protected void addDocument() throws IOException {
        if (currentDocument != null) {
            if (updating) {
                
                writer.updateDocument(new Term(IndexedField.KEY_FIELD, keyEncodedString), 
                                      currentDocument);
                logger.debug("Updated {}", currentDocument);
            }
            else {
                writer.addDocument(currentDocument);
                logger.debug("Added {}", currentDocument);
            }
            documentCount++;
            currentDocument = null;
        }
    }

    protected void getKeyBytes(Row row) {
        
        byte[] bytes = row.hKey().hKeyBytes();
        keyEncodedString = encodeBytes(bytes, 0, bytes.length);
        Field field = new StringField(IndexedField.KEY_FIELD, keyEncodedString, Store.YES);
        currentDocument.add(field);
    }

    protected void addFields(Row row, List<IndexedField> fields) throws IOException {
        if (fields == null) return;
        for (IndexedField indexedField : fields) {
            ValueSource value = row.value(indexedField.getPosition());
            Field field = indexedField.getField(value);
            currentDocument.add(field);
        }
    }

    static String encodeBytes(byte bytes[], int offset, int length)
    {
        // TODO: needs to be more efficient?
        return Strings.toBase64(bytes, offset, length);
    }
    
    static byte[] decodeString(String st)
    {
        return Strings.fromBase64(st);
    }

    @Override
    public void close() {
        Arrays.fill(ancestors, null);
    }

}
