package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface IncomingRelations {

    void forEachIncoming(int nodeId, RelationConsumer consumer);

    Iterator<RelationCursor> incomingIterator(int nodeId);
}
