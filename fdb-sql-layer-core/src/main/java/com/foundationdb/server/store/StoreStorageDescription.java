/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store;

import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.session.Session;

/** Storage associated with a <code>Store</code>. 
*/
public abstract class StoreStorageDescription<SType,SDType> extends StorageDescription
{
    public StoreStorageDescription(HasStorage forObject, String storageFormat) {
        super(forObject, storageFormat);
    }

    public StoreStorageDescription(HasStorage forObject, StoreStorageDescription<SType,SDType> other, String storageFormat) {
        super(forObject, storageFormat);
    }

    /** Create a Row from the current value. */
    public abstract Row expandRow (SType store, Session session, SDType storeData, Schema schema);
    
    /** Store the Row in associated value. */
    public abstract void packRow (SType store, Session session,
                                  SDType storeData, Row row);
    
    
}
