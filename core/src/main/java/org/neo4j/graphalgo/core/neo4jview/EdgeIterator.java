package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.EdgeCursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.RelationshipItem;

class EdgeIterator extends AbstractEdgeIterator<EdgeCursor> {

    private final EdgeCursor cursor = new EdgeCursor();

    public EdgeIterator(long nodeId, ReadOperations read, RelationshipIterator iterator) {
        super(nodeId, read, iterator);
        cursor.sourceVertexId = (int) nodeId;
    }

    @Override
    public EdgeCursor next() {
        final long next = iterator.next();
        final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(next);
        relationshipItemCursor.next();
        final RelationshipItem relationshipItem = relationshipItemCursor.get();
        cursor.targetVertexId = (int) relationshipItem.otherNode(nodeId);
        return cursor;
    }
}