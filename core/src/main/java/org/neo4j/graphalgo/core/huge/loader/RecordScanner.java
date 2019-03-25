package org.neo4j.graphalgo.core.huge.loader;

interface RecordScanner extends Runnable {
    long recordsImported();
}
