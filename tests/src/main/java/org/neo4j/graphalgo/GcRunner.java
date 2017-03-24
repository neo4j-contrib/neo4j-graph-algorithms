package org.neo4j.graphalgo;

interface GcRunner {
    void runGc(String reason) throws InterruptedException;
}
