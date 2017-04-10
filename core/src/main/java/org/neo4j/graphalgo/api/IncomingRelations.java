package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * Iterator for incoming relations based on nodeId
 *
 * @author mknblch
 */
public interface IncomingRelations {

    void forEachIncoming(int nodeId, RelationConsumer consumer);

    Iterator<RelationCursor> incomingIterator(int nodeId);
}
