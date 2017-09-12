package org.neo4j.graphalgo.bench;

import org.neo4j.graphdb.Direction;

public enum DirectionParam {
    IN(Direction.INCOMING),
    OUT(Direction.OUTGOING),
    BOTH(Direction.BOTH),
    NONE(null);

    final Direction direction;

    DirectionParam(Direction direction) {
        this.direction = direction;
    }
}
