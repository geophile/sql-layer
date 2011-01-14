package com.akiban.cserver.itests.bugs.bug696096;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.DuplicateKeyException;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.*;

public final class DuplicateKeyValueMessageTest extends ApiTestBase {
    private TableId tableId;

    @Before
    public void setUp() throws InvalidOperationException {
        tableId = createTable("schema_one", "table_one",
                "c0 INT KEY",
                "c1 int",
                "c2 int",
                "c3 int",
                "UNIQUE (c1, c2)",
                "UNIQUE my_key(c3)"
        );
        writeRows(
                createNewRow(tableId, 0, 1, 2, 3)
        );
    }

    @Test(expected=DuplicateKeyException.class)
    public void primaryDuplicate() {
        expectDuplicateKeyException("PRIMARY", 0, 11, 12, 13);
    }

    @Test(expected=DuplicateKeyException.class)
    public void twoColumnKeyDuplicate() {
        expectDuplicateKeyException("c1", 10, 1, 2, 13);
    }

    @Test(expected=DuplicateKeyException.class)
    public void oneColumnKeyDuplicate() {
        expectDuplicateKeyException("my_key", 10, 11, 12, 3);
    }

    @Test(expected=DuplicateKeyException.class)
    public void multipleDuplications() {
        expectDuplicateKeyException("PRIMARY", 0, 1, 2, 3);
    }

    private static boolean dupMessageValid(DuplicateKeyException e, String indexName) {
        return e.getMessage().contains(indexName);
    }

    private void expectDuplicateKeyException(String indexName, int c0, int c1, int c2, int c3) {
        try {
            writeRows(createNewRow(tableId, c0, c1, c2, c3));
        } catch (DuplicateKeyException e) {
            if (!dupMessageValid(e, indexName)) {
                String errString = String.format("expected message to contain <%s>, but was <%s>",
                        indexName, e.getMessage()
                );
                e.printStackTrace();
                fail(errString);
            }
            return;
        } catch (InvalidOperationException e) {
            throw new TestException("unexpected exception", e);
        }
        fail("Excpected DuplicateKeyExcepton");
    }
}
