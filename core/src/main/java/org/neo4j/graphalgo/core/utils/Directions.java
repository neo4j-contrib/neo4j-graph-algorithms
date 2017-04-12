package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Direction;

/**
 * Utility class for converting neo4j kernel api
 * Direction to user space Direction and back
 *
 * TODO maybe find a better name
 *
 * @author mknblch
 */
public class Directions {

    public static org.neo4j.graphdb.Direction mediate(org.neo4j.storageengine.api.Direction direction) {
        switch (direction) {
            case OUTGOING:
                return Direction.OUTGOING;
            case INCOMING:
                return Direction.INCOMING;
            case BOTH:
                return Direction.BOTH;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static org.neo4j.storageengine.api.Direction mediate(org.neo4j.graphdb.Direction direction) {
        switch (direction) {
            case OUTGOING:
                return org.neo4j.storageengine.api.Direction.OUTGOING;
            case INCOMING:
                return org.neo4j.storageengine.api.Direction.INCOMING;
            case BOTH:
                return org.neo4j.storageengine.api.Direction.BOTH;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
