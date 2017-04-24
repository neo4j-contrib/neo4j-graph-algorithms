package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.ProgressTimer;

/**
 * @author mknblch
 */
public class DijkstraResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Long nodeCount;
    public final Double totalCost;

    public DijkstraResult(Long loadDuration, Long evalDuration, Long writeDuration, Long nodeCount, Double totalCost) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
        this.totalCost = totalCost;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected long loadDuration = -1;
        protected long evalDuration = -1;
        protected long writeDuration = -1;
        protected long nodeCount = 0;
        protected double totalCosts = 0.0;

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

        public Builder withTotalCosts(double totalCosts) {
            this.totalCosts = totalCosts;
            return this;
        }

        public DijkstraResult build() {
            return new DijkstraResult(loadDuration, evalDuration, writeDuration, nodeCount, totalCosts);
        }
    }
}
