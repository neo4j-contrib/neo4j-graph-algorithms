package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.RelationCursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.RelationshipItem;

class RelationIteratorImpl extends AbstractRelationIterator<RelationCursor> {

    private final RelationCursor cursor = new RelationCursor();

    public RelationIteratorImpl(long nodeId, ReadOperations read, RelationshipIterator iterator) {
        super(nodeId, read, iterator);
        cursor.sourceNodeId = (int) nodeId;
    }

    @Override
    public RelationCursor next() {
        final long next = iterator.next();
        final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(next);
        relationshipItemCursor.next();
        final RelationshipItem relationshipItem = relationshipItemCursor.get();
        cursor.targetNodeId = (int) relationshipItem.otherNode(nodeId);
        return cursor;
    }
}