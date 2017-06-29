package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class SimpleSCCResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;

    public SimpleSCCResult(Long loadMillis,
                           Long computeMillis,
                           Long writeMillis) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends AbstractResultBuilder<SimpleSCCResult> {

        private long setCount;
        private long minSetSize;
        private long maxSetSize;

        public Builder withSetCount(long setCount) {
            this.setCount = setCount;
            return this;
        }

        public Builder withMinSetSize(long minSetSize) {
            this.minSetSize = minSetSize;
            return this;
        }

        public Builder withMaxSetSize(long maxSetSize) {
            this.maxSetSize = maxSetSize;
            return this;
        }

        @Override
        public SimpleSCCResult build() {
            return new SimpleSCCResult(loadDuration,
                    evalDuration,
                    writeDuration);
        }
    }

}
