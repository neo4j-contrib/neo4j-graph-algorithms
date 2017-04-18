package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * Iterator for both-relations based on nodeId
 *
 * @author mknblch
 */
public interface BothRelationshipIterator {

    void forEachRelationship(int nodeId, RelationshipConsumer consumer);

    Iterator<RelationshipCursor> bothRelationshipIterator(int nodeId);
}
