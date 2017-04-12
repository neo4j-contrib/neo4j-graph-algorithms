package org.neo4j.graphalgo.core.sources;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.RelationshipContainer;
import org.neo4j.graphalgo.core.utils.WeightedRelationshipContainer;

import java.util.Iterator;

/**
 * Adapter class for WeightedRelationshipContainer -> WeightedIncomingRelationIterator
 *
 * @author mknblch
 */
public class WeightedIncomingRelationshipAdapter implements WeightedIncomingRelationshipIterator {

    private final WeightedRelationshipContainer container;

    public WeightedIncomingRelationshipAdapter(WeightedRelationshipContainer container) {
        this.container = container;
    }

    @Override
    public void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, consumer);
    }

    @Override
    public Iterator<WeightedRelationshipCursor> incomingWeightedIterator(int nodeId) {
        return container.iterator(nodeId);
    }
}
