package org.neo4j.graphalgo.core.sources;

import org.neo4j.graphalgo.api.OutgoingRelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;

import java.util.Iterator;

/**
 * Adapter class for RelationContainer -> OutgoingRelationContainer
 *
 * @author mknblch
 */
public class OutgoingRelationshipAdapter implements OutgoingRelationshipIterator {

    private final RelationshipContainer container;

    public OutgoingRelationshipAdapter(RelationshipContainer container) {
        this.container = container;
    }

    @Override
    public void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        container.forEach(nodeId, consumer);
    }

    @Override
    public Iterator<RelationshipCursor> outgoingIterator(int nodeId) {
        return container.iterator(nodeId);
    }
}
