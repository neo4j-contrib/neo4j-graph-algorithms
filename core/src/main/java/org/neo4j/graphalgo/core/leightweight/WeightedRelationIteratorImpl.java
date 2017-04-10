package org.neo4j.graphalgo.core.leightweight;


import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipCursor;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class WeightedRelationIteratorImpl implements Iterator<WeightedRelationshipCursor> {

    private final WeightedRelationshipCursor cursor = new WeightedRelationshipCursor();
    private final IntArray.Cursor adjCursor;
    private final WeightMapping weightMapping;

    private final LongLongMap relationIdMapping;
    private long relationId;

    private int[] array;
    private int pos;
    private int limit;

    WeightedRelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            WeightMapping weightMapping,
            LongLongMap relationIdMapping,
            IntArray adjacency) {
        this(sourceNodeId,
                offset,
                length,
                weightMapping,
                relationIdMapping,
                adjacency,
                adjacency.newCursor());
    }

    WeightedRelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            WeightMapping weightMapping,
            LongLongMap relationIdMapping,
            IntArray adjacency,
            IntArray.Cursor adjCursor) {
        this.weightMapping = weightMapping;
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
    public WeightedRelationshipCursor next() {
        cursor.weight = weightMapping.get(relationId);
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
