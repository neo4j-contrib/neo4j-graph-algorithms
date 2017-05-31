package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class RelationIteratorImpl implements Iterator<RelationshipCursor> {

    private final RelationshipCursor cursor = new RelationshipCursor();
    private final IntArray.Cursor adjCursor;

    private final IdCombiner relId;

    private int[] array;
    private int pos;
    private int limit;

    RelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            IntArray adjacency,
            Direction direction) {
        this(sourceNodeId, offset, length, adjacency, adjacency.newCursor(), direction);
    }

    RelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            IntArray adjacency,
            IntArray.Cursor adjCursor,
            Direction direction) {
        cursor.sourceNodeId = sourceNodeId;
        this.adjCursor = adjacency.cursor(offset, length, adjCursor);
        relId = RawValues.combiner(direction);
        nextPage();
    }

    @Override
    public boolean hasNext() {
        return pos < limit || nextPage();
    }

    @Override
    public RelationshipCursor next() {
        cursor.targetNodeId = array[pos++];
        cursor.relationshipId = relId.apply(cursor);
        return cursor;
    }

    private boolean nextPage() {
        if (adjCursor.next()) {
            array = adjCursor.array;
            pos = adjCursor.offset;
            limit = adjCursor.offset + adjCursor.length;
            return true;
        }
        pos = limit = 0;
        return false;
    }
}
