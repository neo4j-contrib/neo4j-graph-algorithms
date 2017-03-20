package org.neo4j.graphalgo.extra;

interface GcRunner {
    void runGc(String reason) throws InterruptedException;
}
