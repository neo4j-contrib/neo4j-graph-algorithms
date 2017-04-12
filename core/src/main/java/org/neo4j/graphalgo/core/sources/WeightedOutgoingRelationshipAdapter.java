package org.neo4j.graphalgo.core.sources;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.RelationshipContainer;
import org.neo4j.graphalgo.core.utils.WeightedRelationshipContainer;

import java.util.Iterator;

/**
 * Adapter class for WeightedRelationContainer -> WeightedOutgoingRelationContainer
 *
 * @author mknblch
 */
public class WeightedOutgoingRelationshipAdapter implements WeightedOutgoingRelationshipIterator {

    private final WeightedRelationshipContainer container;

    public WeightedOutgoingRelationshipAdapter(WeightedRelationshipContainer container) {
        this.container = container;
    }

    @Override
    public void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, consumer);
    }

    @Override
    public Iterator<WeightedRelationshipCursor> outgoingWeightedIterator(int nodeId) {
        return container.iterator(nodeId);
    }
}
