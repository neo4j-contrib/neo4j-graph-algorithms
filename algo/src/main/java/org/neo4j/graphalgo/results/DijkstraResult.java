package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class DijkstraResult {

    public final long loadMillis;
    public final long evalMillis;
    public final long writeMillis;
    public final long nodeCount;
    public final double totalCost;

    public DijkstraResult(long loadMillis, long evalMillis, long writeMillis, long nodeCount, double totalCost) {
        this.loadMillis = loadMillis;
        this.evalMillis = evalMillis;
        this.writeMillis = writeMillis;
        this.nodeCount = nodeCount;
        this.totalCost = totalCost;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<DijkstraResult>{

        protected long nodeCount = 0;
        protected double totalCosts = 0.0;

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
