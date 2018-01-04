package org.neo4j.graphalgo.impl.scc;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.SCCResult;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public interface SCCAlgorithm {

    SCCAlgorithm compute();

    Stream<SCCAlgorithm.Result> resultStream();

    SCCAlgorithm withProgressLogger(ProgressLogger wrap);

    SCCAlgorithm withTerminationFlag(TerminationFlag wrap);

    SCCAlgorithm release();

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

        public Result(Long loadMillis,
                         Long computeMillis,
                         Long writeMillis
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
        }

        public static org.neo4j.graphalgo.results.SCCResult.Builder builder() {
            return new org.neo4j.graphalgo.results.SCCResult.Builder();
        }

        public static final class Builder extends AbstractResultBuilder<Result> {

            @Override
            public Result build() {
                return new Result(loadDuration,
                        evalDuration,
                        writeDuration);
            }
        }

    }

}
