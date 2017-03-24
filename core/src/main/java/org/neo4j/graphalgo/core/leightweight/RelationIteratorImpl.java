package org.neo4j.graphalgo.core.leightweight;



import org.neo4j.graphalgo.api.RelationCursor;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class RelationIteratorImpl implements Iterator<RelationCursor> {

    private final RelationCursor cursor = new RelationCursor();
    private final IntArray.Cursor adjCursor;

    private long relationId;

    private int[] array;
    private int pos;
    private int limit;

    RelationIteratorImpl(
        int sourceNodeId,
        long offset,
        long length,
        IntArray adjacency) {
        this(sourceNodeId, offset, length, adjacency, adjacency.newCursor());
    }

    RelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        relationId = offset;
        cursor.sourceNodeId = sourceNodeId;
        this.adjCursor = adjacency.cursor(offset, length, adjCursor);
        nextPage();
    }

    @Override
    public boolean hasNext() {
        return pos < limit || nextPage();
    }

    @Override
    public RelationCursor next() {
        cursor.relationId = relationId++;
        cursor.targetNodeId = array[pos++];
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
