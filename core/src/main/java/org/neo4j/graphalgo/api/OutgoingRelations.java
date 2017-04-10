package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * @author mknblch
 *         added 10.04.2017.
 */
public interface OutgoingRelations {

    void forEachOutgoing(int nodeId, RelationConsumer consumer);

    Iterator<RelationCursor> outgoingIterator(int nodeId);
}
