package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphdb.Direction;

import java.util.concurrent.ArrayBlockingQueue;

final class RelationshipsBatch implements AutoCloseable {
    static final RelationshipsBatch SENTINEL = new RelationshipsBatch(null);

    long[] sourceAndTargets;
    int length;
    Direction direction;

    private final ArrayBlockingQueue<RelationshipsBatch> pool;

    RelationshipsBatch(final ArrayBlockingQueue<RelationshipsBatch> pool) {
        this.pool = pool;
    }

    @Override
    public void close() {
        if (pool != null) {
            returnToPool();
        }
    }

    private void returnToPool() {
        pool.add(this);
//        while (true) {
//            if (pool.offer(this)) {
//                return;
//            }
//        }
    }
}
