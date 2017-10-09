package org.neo4j.graphalgo.impl.msbfs;

import java.util.concurrent.ExecutorService;

public interface MsBFSAlgo {
    void run(int concurrency, ExecutorService executor);
}
