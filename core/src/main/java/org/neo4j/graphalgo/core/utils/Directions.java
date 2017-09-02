package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Direction;

/**
 * Utility class for converting neo4j kernel api
 * Direction to user space Direction and back
 * <p>
 *
 * @author mknblch
 */
public class Directions {

    public static final Direction DEFAULT_DIRECTION = Direction.OUTGOING;

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

    public static Direction fromString(String directionString) {
        return fromString(directionString, DEFAULT_DIRECTION);
    }

    public static Direction fromString(String directionString, Direction defaultDirection) {

        if (null == directionString) {
            return defaultDirection;
        }

        switch (directionString.toLowerCase()) {

            case "outgoing":
            case "out":
            case "o":
            case ">" :
                return Direction.OUTGOING;

            case "incoming":
            case "in":
            case "i":
            case "<":
                return Direction.INCOMING;

            case "both":
            case "b":
            case "<>":
                return Direction.BOTH;

            default:
                return defaultDirection;
        }
    }
}
