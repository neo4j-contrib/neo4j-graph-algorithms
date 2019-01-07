package org.neo4j.internal.kernel.api.helpers;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.loading.NodesHelper.countUndirected;

public final class NodesHelperTest {

    @Test
    public void shouldCountUndirectedDense() {
        StubGroupCursor groupCursor = new StubGroupCursor(
                group().withOutCount(1).withInCount(1).withLoopCount(5),
                group().withOutCount(1).withInCount(1).withLoopCount(3),
                group().withOutCount(2).withInCount(1).withLoopCount(2),
                group().withOutCount(3).withInCount(1).withLoopCount(1),
                group().withOutCount(5).withInCount(1).withLoopCount(1)
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors(groupCursor);

        int count = countUndirected(new StubNodeCursor(true), cursors);

        assertEquals(41, count);
    }

    @Test
    public void shouldCountUndirectedSparse() {
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain(11)
                        .outgoing(55, 0, 1)
                        .incoming(56, 0, 1)
                        .outgoing(57, 0, 1)
                        .loop(58, 0));
        StubCursorFactory cursors = new StubCursorFactory().withRelationshipTraversalCursors(relationshipCursor);

        StubNodeCursor nodeCursor = new StubNodeCursor(false).withNode(11);
        nodeCursor.next();

        int count = countUndirected(nodeCursor, cursors);

        assertEquals(5, count);
    }

    @Test
    public void shouldCountUndirectedWithTypeDense() {
        StubGroupCursor groupCursor = new StubGroupCursor(
                group(1).withOutCount(1).withInCount(1).withLoopCount(5),
                group(2).withOutCount(1).withInCount(1).withLoopCount(3)
        );
        StubCursorFactory cursors = new StubCursorFactory().withGroupCursors(groupCursor, groupCursor);

        int countType1 = countUndirected(new StubNodeCursor(true), cursors, 1);
        int countType2 = countUndirected(new StubNodeCursor(true), cursors, 2);

        assertEquals(12, countType1);
        assertEquals(8, countType2);
    }

    @Test
    public void shouldCountUndirectedWithTypeSparse() {
        StubRelationshipCursor relationshipCursor = new StubRelationshipCursor(
                new TestRelationshipChain(11)
                        .outgoing(55, 0, 1)
                        .incoming(56, 0, 1)
                        .outgoing(57, 0, 1)
                        .loop(58, 1)
                        .incoming(59, 0, 2)
                        .outgoing(60, 0, 2)
                        .loop(61, 2)
                        .loop(62, 2));
        StubCursorFactory cursors = new StubCursorFactory(true)
                .withRelationshipTraversalCursors(relationshipCursor);

        StubNodeCursor nodeCursor = new StubNodeCursor(false).withNode(11);
        nodeCursor.next();

        int countType1 = countUndirected(nodeCursor, cursors, 1);
        int countType2 = countUndirected(nodeCursor, cursors, 2);

        assertEquals(5, countType1);
        assertEquals(6, countType2);
    }

    private StubGroupCursor.GroupData group() {
        return new StubGroupCursor.GroupData(0, 0, 0, 0);
    }

    private StubGroupCursor.GroupData group(int type) {
        return new StubGroupCursor.GroupData(0, 0, 0, type);
    }
}
