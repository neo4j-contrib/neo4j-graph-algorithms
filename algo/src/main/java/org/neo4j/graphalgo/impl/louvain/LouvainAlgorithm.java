package org.neo4j.graphalgo.impl.louvain;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface LouvainAlgorithm {

    LouvainAlgorithm compute();

    int[] getCommunityIds();

    int getIterations();

    int getCommunityCount() ;

    Stream<Result> resultStream();

    class Result {

        public final long nodeId;
        public final long community;

        public Result(long nodeId, int community) {
            this.nodeId = nodeId;
            this.community = community;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", community=" + community +
                    '}';
        }
    }
}
