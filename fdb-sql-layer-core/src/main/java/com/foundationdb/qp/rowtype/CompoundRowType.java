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
package com.foundationdb.qp.rowtype;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.types.TInstance;

public class CompoundRowType extends DerivedRowType {

    @Override
    public int nFields() {
        return nFields;
    }

    @Override
    public TInstance typeAt(int index) {
        if (index < first.nFields())
            return first.typeAt(index);
        return second.typeAt(index - first.nFields());
    }
    
    public RowType first() {
        return first;
    }
    
    public RowType second() {
        return second;
    }
    
    protected CompoundRowType(Schema schema, int typeId, RowType first, RowType second) {
        super(schema, typeId);

        assert first.schema() == schema : first;
        assert second.schema() == schema : second;
        
        this.first = first;
        this.second = second; 
        this.nFields = first.nFields() + second.nFields();

        List<Table> tables = new ArrayList<>();
        if(first.typeComposition() != null) {
            tables.addAll(first.typeComposition().tables());
        }
        if(second.typeComposition() != null) {
            tables.addAll(second.typeComposition().tables());
        }
        if(!tables.isEmpty()) {
            typeComposition(new TypeComposition(this, tables));
        }
    }
    
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CompoundRowType that = (CompoundRowType) o;

        if (second != null ? !second.equals(that.second) : that.second != null) return false;
        if (first != null ? !first.equals(that.first) : that.first != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (first != null ? first.hashCode() : 0);
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }
    
    private final RowType first;
    private final RowType second;
    protected int nFields;

}
