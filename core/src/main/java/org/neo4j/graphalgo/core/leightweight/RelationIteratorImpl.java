package org.neo4j.graphalgo.core.leightweight;



import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.api.RelationshipCursor;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class RelationIteratorImpl implements Iterator<RelationshipCursor> {

    private final RelationshipCursor cursor = new RelationshipCursor();
    private final IntArray.Cursor adjCursor;

    private final LongLongMap relationIdMapping;
    private long relationId;

    private int[] array;
    private int pos;
    private int limit;

    RelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            LongLongMap relationIdMapping,
            IntArray adjacency) {
        this(sourceNodeId, offset, length, relationIdMapping, adjacency, adjacency.newCursor());
    }

    RelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            LongLongMap relationIdMapping,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        relationId = offset;
        this.relationIdMapping = relationIdMapping;
        cursor.sourceNodeId = sourceNodeId;
        this.adjCursor = adjacency.cursor(offset, length, adjCursor);
        nextPage();
    }

    @Override
    public boolean hasNext() {
        return pos < limit || nextPage();
    }

    @Override
    public RelationshipCursor next() {
        cursor.relationId = relationIdMapping.get(relationId++);
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
