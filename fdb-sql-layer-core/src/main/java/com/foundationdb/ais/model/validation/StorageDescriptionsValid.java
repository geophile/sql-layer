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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.error.StorageDescriptionInvalidException;

import java.util.Collection;

/**
 * Check all <code>StorageDescription</code>s are present and valid.
 */
public class StorageDescriptionsValid implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for(Group group : ais.getGroups().values()) {
            checkObject(group, output);
            for(Index index : group.getIndexes()) {
                checkObject(index, output);
            }
        }
        for(Table table : ais.getTables().values()) {
            for(Index index : table.getIndexesIncludingInternal()) {
                checkObject(index, output);
            }
        }
        for (Sequence sequence: ais.getSequences().values()) {
            checkObject(sequence, output);
        }
    }

    private static void checkObject(HasStorage object, AISValidationOutput output) {
        if(object.getStorageDescription() == null) {
            output.reportFailure(new AISValidationFailure(new StorageDescriptionInvalidException(object, "has not been set")));
        }
        else {
            object.getStorageDescription().validate(output);
        }
    }
}
