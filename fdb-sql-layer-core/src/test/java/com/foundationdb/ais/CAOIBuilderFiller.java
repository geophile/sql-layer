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
package com.foundationdb.ais;

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

public class CAOIBuilderFiller {
    public final static String CUSTOMER_TABLE = "customer";
    public final static String ADDRESS_TABLE = "address";
    public final static String ORDER_TABLE = "order";
    public final static String ITEM_TABLE = "item";
    public final static String COMPONENT_TABLE = "component";

    public static NewAISBuilder createAndFillBuilder(String schema) {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        NewAISBuilder builder = AISBBasedBuilder.create(schema, typesTranslator);

        builder.table(CUSTOMER_TABLE).
                colBigInt("customer_id", false).
                colString("customer_name", 100, false).
                pk("customer_id");

        builder.table(ADDRESS_TABLE).
                colBigInt("customer_id", false).
                colInt("instance_id", false).
                colString("address_line1", 60, false).
                colString("address_line2", 60, false).
                colString("address_line3", 60, false).
                pk("customer_id", "instance_id").
                joinTo("customer").on("customer_id", "customer_id");

        builder.table(ORDER_TABLE).
                colBigInt("order_id", false).
                colBigInt("customer_id", false).
                colInt("order_date", false).
                pk("order_id").
                joinTo("customer").on("customer_id", "customer_id");

        builder.table(ITEM_TABLE).
                colBigInt("order_id", false).
                colBigInt("part_id", false).
                colInt("quantity", false).
                colInt("unit_price", false).
                pk("part_id").
                joinTo("order").on("order_id", "order_id");

        builder.table(COMPONENT_TABLE).
                colBigInt("part_id", false).
                colBigInt("component_id", false).
                colInt("supplier_id", false).
                colInt("unique_id", false).
                colString("description", 50, true).
                pk("component_id").
                uniqueKey("uk", "unique_id").
                key("xk", "supplier_id").
                joinTo("item").on("part_id", "part_id");

        return builder;
    }
}
