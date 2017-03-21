package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.WeightedRelationCursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.RelationshipItem;

class WeightedRelationIteratorImpl extends AbstractRelationIterator<WeightedRelationCursor> {

    private final WeightedRelationCursor cursor = new WeightedRelationCursor();
    private final int propertyKey;

    public WeightedRelationIteratorImpl(long nodeId, ReadOperations read, RelationshipIterator iterator, int propertyKey) {
        super(nodeId, read, iterator);
        this.propertyKey = propertyKey;
        cursor.sourceNodeId = (int) nodeId;
    }

    @Override
    public WeightedRelationCursor next() {
        final long next = iterator.next();
        final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(next);
        relationshipItemCursor.next();
        final RelationshipItem relationshipItem = relationshipItemCursor.get();
        cursor.targetNodeId = (int) relationshipItem.otherNode(nodeId);
        try {
            final Object o = read.relationshipGetProperty(next, propertyKey);
            if (null == o) {
                cursor.weight = 0;
            } else {
                cursor.weight = (double) o;
            }
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }
}