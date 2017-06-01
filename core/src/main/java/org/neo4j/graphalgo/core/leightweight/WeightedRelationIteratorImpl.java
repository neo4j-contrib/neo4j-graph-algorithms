package org.neo4j.graphalgo.core.leightweight;


import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipCursor;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author phorn@avantgarde-labs.de
 */
class WeightedRelationIteratorImpl implements Iterator<WeightedRelationshipCursor> {

    private final WeightedRelationshipCursor cursor = new WeightedRelationshipCursor();
    private final IntArray.Cursor adjCursor;
    private final WeightMapping weightMapping;
    private final IdCombiner relId;

    private long relationId;

    private int[] array;
    private int pos;
    private int limit;

    WeightedRelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            WeightMapping weightMapping,
            IntArray adjacency,
            Direction direction) {
        this(sourceNodeId,
                offset,
                length,
                weightMapping,
                adjacency,
                adjacency.newCursor(),
                direction);
    }

    WeightedRelationIteratorImpl(
            int sourceNodeId,
            long offset,
            long length,
            WeightMapping weightMapping,
            IntArray adjacency,
            IntArray.Cursor adjCursor,
            Direction direction) {
        this.weightMapping = weightMapping;
        relationId = offset;
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
    public WeightedRelationshipCursor next() {
        cursor.weight = weightMapping.get(relationId++);
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
