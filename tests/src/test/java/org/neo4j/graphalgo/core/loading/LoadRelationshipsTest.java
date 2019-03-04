package org.neo4j.graphalgo.core.loading;

import org.junit.Test;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.StubCursorFactory;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;

import static org.junit.Assert.assertEquals;

public class LoadRelationshipsTest {
    @Test
    public void allRelationshipsUnique() {
        StubNodeCursor stubNodeCursor = new StubNodeCursor(false);

        StubCursorFactory stubCursorFactory = new StubCursorFactory();

        TestRelationshipChain chain = new TestRelationshipChain(0)
                .outgoing(0, 3, 0)
                .outgoing(1, 4, 0);

        StubRelationshipCursor stubRelationshipCursor = new StubRelationshipCursor(chain);
        stubCursorFactory.withRelationshipTraversalCursors(stubRelationshipCursor);

        LoadAllRelationships loadAllRelationships = new LoadAllRelationships(stubCursorFactory);

        int degreeBoth = loadAllRelationships.degreeBoth(stubNodeCursor);
        assertEquals(2, degreeBoth);
    }

    @Test
    public void countMultipleRelationshipsBetweenNodesOnce() {
        StubNodeCursor stubNodeCursor = new StubNodeCursor(false);

        StubCursorFactory stubCursorFactory = new StubCursorFactory();

        TestRelationshipChain node0Chain = new TestRelationshipChain(0)
                .outgoing(0, 1, 0)
                .incoming(1, 1, 0);
        RelationshipTraversalCursor node0Cursor = new StubRelationshipCursor(node0Chain);

        stubCursorFactory.withRelationshipTraversalCursors(node0Cursor);

        LoadAllRelationships loadAllRelationships = new LoadAllRelationships(stubCursorFactory);

        int degreeBoth = loadAllRelationships.degreeBoth(stubNodeCursor);
        assertEquals(1, degreeBoth);
    }

    @Test
    public void countMultipleRelationshipsBetweenNodesOnceType() {
        StubNodeCursor stubNodeCursor = new StubNodeCursor(false);

        StubCursorFactory stubCursorFactory = new StubCursorFactory();

        TestRelationshipChain node0Chain = new TestRelationshipChain(0)
                .outgoing(0, 1, 1)
                .outgoing(1, 2, 2)
                .incoming(2, 1, 1);
        RelationshipTraversalCursor node0Cursor = new StubRelationshipCursor(node0Chain);

        stubCursorFactory.withRelationshipTraversalCursors(node0Cursor);

        LoadRelationshipsOfSingleType loadAllRelationships = new LoadRelationshipsOfSingleType(stubCursorFactory, new int[] {1});

        int degreeBoth = loadAllRelationships.degreeBoth(stubNodeCursor);
        assertEquals(1, degreeBoth);
    }


}
