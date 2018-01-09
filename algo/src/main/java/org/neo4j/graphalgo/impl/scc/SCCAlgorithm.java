package org.neo4j.graphalgo.impl.scc;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.SCCResult;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface SCCAlgorithm {

    SCCAlgorithm compute();

    long getSetCount();

    long getMinSetSize();

    long getMaxSetSize();

    Stream<SCCAlgorithm.StreamResult>  resultStream();

    SCCAlgorithm withProgressLogger(ProgressLogger wrap);

    SCCAlgorithm withTerminationFlag(TerminationFlag wrap);

    SCCAlgorithm release();

    <V> V getConnectedComponents();

    class StreamResult {

        public final long nodeId;
        public final long partition;

        public StreamResult(long nodeId, long partition) {
            this.nodeId = nodeId;
            this.partition = partition;
        }
    }

    class Result {

        public final Long loadMillis;
        public final Long computeMillis;
        public final Long writeMillis;
        public final Long setCount;
        public final Long minSetSize;
        public final Long maxSetSize;

        public Result(Long loadMillis,
                         Long computeMillis,
                         Long writeMillis,
                         Long setCount,
                         Long minSetSize,
                         Long maxSetSize) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.setCount = setCount;
            this.minSetSize = minSetSize;
            this.maxSetSize = maxSetSize;
        }

        public static Result.Builder builder() {
            return new Result.Builder();
        }

        public static final class Builder extends AbstractResultBuilder<Result> {

            private long setCount;
            private long minSetSize;
            private long maxSetSize;

            public Result.Builder withSetCount(long setCount) {
                this.setCount = setCount;
                return this;
            }

            public Result.Builder withMinSetSize(long minSetSize) {
                this.minSetSize = minSetSize;
                return this;
            }

            public Result.Builder withMaxSetSize(long maxSetSize) {
                this.maxSetSize = maxSetSize;
                return this;
            }

            @Override
            public Result build() {
                return new Result(loadDuration,
                        evalDuration,
                        writeDuration,
                        setCount,
                        minSetSize,
                        maxSetSize);
            }
        }

    }

    static SCCAlgorithm iterativeTarjan(Graph graph, AllocationTracker tracker) {
        if (graph instanceof HugeGraph) {
            return new HugeSCCIterativeTarjan((HugeGraph) graph, tracker);
        }
        return new SCCIterativeTarjan(graph);
    }
}
