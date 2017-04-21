package org.neo4j.graphalgo.core.utils.dss;

import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
public class DssStats {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Long nodeCount;
    public final Long setCount;

    protected DssStats(Long loadDuration, Long evalDuration, Long writeDuration, Long nodeCount, Long setCount) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
        this.setCount = setCount;
    }

    @Override
    public String toString() {
        return "DssStats{" +
                "load=" + loadDuration +
                "ms, eval=" + evalDuration +
                "ms, write=" + writeDuration +
                "ms, nodeCount=" + nodeCount +
                ", setCount=" + setCount +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long loadTime = -1;
        private long evalTime = -1;
        private long writeTime = -1;
        private long nodeCount = 0;
        private long setCount = 0;

        public Builder startLoad() {
            loadTime = System.nanoTime();
            return this;
        }

        public Builder stopLoad() {
            loadTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadTime);
            return this;
        }

        public Builder startEval() {
            evalTime = System.nanoTime();
            return this;
        }

        public Builder stopEval() {
            evalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - evalTime);
            return this;
        }

        public Builder startWrite() {
            writeTime = System.nanoTime();
            return this;
        }

        public Builder stopWrite() {
            writeTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeTime);
            return this;
        }

        public Builder withSetCount(long setCount) {
            this.setCount = setCount;
            return this;
        }

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public DssStats build() {
            return new DssStats(loadTime, evalTime, writeTime, nodeCount, setCount);
        }
    }
}
