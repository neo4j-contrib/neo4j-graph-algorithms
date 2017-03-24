package org.neo4j.graphalgo.core.leightweight;


import org.neo4j.graphalgo.api.WeightedRelationCursor;
import org.neo4j.graphalgo.core.WeightMapping;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class WeightedRelationIteratorImpl implements Iterator<WeightedRelationCursor> {

    private final WeightedRelationCursor cursor = new WeightedRelationCursor();
    private final IntArray.Cursor adjCursor;
    private final WeightMapping weightMapping;

    private long relationId;

    private int[] array;
    private int pos;
    private int limit;

    WeightedRelationIteratorImpl(
        int sourceNodeId,
        long offset,
        long length,
        WeightMapping weightMapping,
        IntArray adjacency) {
        this(sourceNodeId,
                offset,
                length,
                weightMapping,
                adjacency,
                adjacency.newCursor());
    }

    WeightedRelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            WeightMapping weightMapping,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        this.weightMapping = weightMapping;
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
    public WeightedRelationCursor next() {
        cursor.weight = weightMapping.get(relationId);
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
