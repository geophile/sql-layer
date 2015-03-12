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
package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexRowComposition;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.TableRowType;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.ancestorLookup_Default;
import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertEquals;

// Inspired by bug 987942

public class GroupIndexRowIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        user = createTable(
            "schema", "usr",
            "uid int not null",
            "primary key(uid)");
        memberInfo = createTable(
            "schema", "member_info",
            "profileID int not null",
            "lastLogin int",
            "primary key(profileId)",
            "grouping foreign key (profileID) references usr(uid)");
        entitlementUserGroup = createTable(
            "schema", "entitlement_user_group",
            "entUserGroupID int not null",
            "uid int",
            "primary key(entUserGroupID)",
            "grouping foreign key (uid) references member_info(profileID)");
        createLeftGroupIndex(new TableName("schema", "usr"), "gi", "entitlement_user_group.uid", "member_info.lastLogin");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        userRowType = schema.tableRowType(table(user));
        memberInfoRowType = schema.tableRowType(table(memberInfo));
        entitlementUserGroupRowType = schema.tableRowType(table(entitlementUserGroup));
        groupIndexRowType = groupIndexType(Index.JoinType.LEFT, "entitlement_user_group.uid", "member_info.lastLogin");
        group = group(user);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(user, 1L),
            row(memberInfo, 1L, 20120424),
        };
        use(db);
    }

    @Test
    public void testIndexMetadata()
    {
        // Index row: e.uid, m.lastLogin, m.profileID, e.eugid
        // HKey for eug table: [U, e.uid, M, E, e.eugid]
        GroupIndex gi = (GroupIndex) groupIndexRowType.index();
        IndexRowComposition rowComposition = gi.indexRowComposition();
        assertEquals(4, rowComposition.getFieldPosition(0));
        assertEquals(2, rowComposition.getFieldPosition(1));
        assertEquals(1, rowComposition.getFieldPosition(2));
        assertEquals(3, rowComposition.getFieldPosition(3));
    }

    @Test
    public void testItemIndexToMissingCustomerAndOrder()
    {
        Operator indexScan = indexScan_Default(groupIndexRowType,
                                               IndexKeyRange.unbounded(groupIndexRowType),
                                               new API.Ordering(),
                                               memberInfoRowType);
        Operator plan =
            ancestorLookup_Default(
                indexScan,
                group,
                groupIndexRowType,
                Arrays.asList(userRowType, memberInfoRowType),
                API.InputPreservationOption.DISCARD_INPUT);
        Row[] expected = new Row[] {
            row(userRowType, 1L),
            row(memberInfoRowType, 1L, 20120424L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }


    private int user;
    private int memberInfo;
    private int entitlementUserGroup;
    private TableRowType userRowType;
    private TableRowType memberInfoRowType;
    private TableRowType entitlementUserGroupRowType;
    private IndexRowType groupIndexRowType;
    private Group group;
}
