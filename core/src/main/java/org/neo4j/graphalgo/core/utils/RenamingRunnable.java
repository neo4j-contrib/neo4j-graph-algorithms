package org.neo4j.graphalgo.core.utils;

public interface RenamingRunnable extends Runnable {

    @Override
    default void run() {
        Thread currentThread = Thread.currentThread();
        String oldThreadName = currentThread.getName();
        String newThreadName = threadName();

        boolean renamed = false;
        if (!oldThreadName.equals(newThreadName)) {
            try {
                currentThread.setName(newThreadName);
                renamed = true;
            } catch (SecurityException e) {
                // failed to rename thread, proceed as usual
            }
        }

        try {
            doRun();
        } finally {
            if (renamed) {
                currentThread.setName(oldThreadName);
            }
        }
    }

    default String threadName() {
        return getClass().getSimpleName() + "-" + System.identityHashCode(this);
    }

    void doRun();
}
