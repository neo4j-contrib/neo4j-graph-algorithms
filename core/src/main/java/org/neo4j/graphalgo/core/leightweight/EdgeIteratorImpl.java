package org.neo4j.graphalgo.core.leightweight;



import org.neo4j.graphalgo.api.EdgeCursor;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class EdgeIteratorImpl implements Iterator<EdgeCursor> {

    private final EdgeCursor cursor = new EdgeCursor();
    private final IntArray.Cursor adjCursor;

    private long edgeId;

    private int[] array;
    private int pos;
    private int limit;

    EdgeIteratorImpl(
        int sourceVertexId,
        long offset,
        long length,
        IntArray adjacency) {
        this(sourceVertexId, offset, length, adjacency, adjacency.newCursor());
    }

    EdgeIteratorImpl(
            int sourceVertexId,
            long offset,
            long length,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        edgeId = offset;
        cursor.sourceVertexId = sourceVertexId;
        this.adjCursor = adjacency.cursor(offset, length, adjCursor);
        nextPage();
    }

    @Override
    public boolean hasNext() {
        return pos < limit || nextPage();
    }

    @Override
    public EdgeCursor next() {
        cursor.edgeId = edgeId++;
        cursor.targetVertexId = array[pos++];
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
