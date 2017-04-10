package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * Iterator for incoming relations based on nodeId
 *
 * @author mknblch
 */
public interface IncomingRelationshipIterator {

    void forEachIncoming(int nodeId, RelationshipConsumer consumer);

    Iterator<RelationshipCursor> incomingIterator(int nodeId);
}
