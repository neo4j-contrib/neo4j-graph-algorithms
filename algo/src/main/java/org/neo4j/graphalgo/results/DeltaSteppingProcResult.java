package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.ProgressTimer;

/**
 * @author mknblch
 */
public class DeltaSteppingProcResult {

    public final long loadDuration;
    public final long evalDuration;
    public final long writeDuration;
    public final long nodeCount;

    public DeltaSteppingProcResult(long loadDuration, long evalDuration, long writeDuration, long nodeCount) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<DeltaSteppingProcResult> {

        protected long nodeCount = 0;

        public ProgressTimer load() {
            return ProgressTimer.start(res -> loadDuration = res);
        }

        public ProgressTimer eval() {
            return ProgressTimer.start(res -> evalDuration = res);
        }

        public ProgressTimer write() {
            return ProgressTimer.start(res -> writeDuration = res);
        }

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public DeltaSteppingProcResult build() {
            return new DeltaSteppingProcResult(loadDuration, evalDuration, writeDuration, nodeCount);
        }
    }
}
