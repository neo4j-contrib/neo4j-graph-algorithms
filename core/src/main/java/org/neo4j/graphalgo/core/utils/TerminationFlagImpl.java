package org.neo4j.graphalgo.core.utils;

import org.neo4j.kernel.api.KernelTransaction;

/**
 * @author mknblch
 */
public class TerminationFlagImpl implements TerminationFlag {

    private final KernelTransaction transaction;

    private long interval = 10_000;

    private volatile long lastCheck = 0;

    private volatile boolean running = true;

    public TerminationFlagImpl(KernelTransaction transaction) {
        this.transaction = transaction;
    }

    public TerminationFlagImpl withCheckInterval(long interval) {
        this.interval = interval;
        return this;
    }

    @Override
    public boolean running() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime > lastCheck + interval) {
            if (transaction.getReasonIfTerminated() != null || !transaction.isOpen()) {
                running = false;
            }
            lastCheck = currentTime;
        }
        return running;
    }
}
