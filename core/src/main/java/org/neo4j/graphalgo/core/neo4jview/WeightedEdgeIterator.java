package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.WeightedEdgeCursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.storageengine.api.RelationshipItem;

class WeightedEdgeIterator extends AbstractEdgeIterator<WeightedEdgeCursor> {

    private final WeightedEdgeCursor cursor = new WeightedEdgeCursor();
    private final int propertyKey;

    public WeightedEdgeIterator(long nodeId, ReadOperations read, RelationshipIterator iterator, int propertyKey) {
        super(nodeId, read, iterator);
        this.propertyKey = propertyKey;
        cursor.sourceVertexId = (int) nodeId;
    }

    @Override
    public WeightedEdgeCursor next() {
        final long next = iterator.next();
        final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(next);
        relationshipItemCursor.next();
        final RelationshipItem relationshipItem = relationshipItemCursor.get();
        cursor.targetVertexId = (int) relationshipItem.otherNode(nodeId);
        try {
            cursor.weight = (double) read.relationshipGetProperty(next, propertyKey);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }
}