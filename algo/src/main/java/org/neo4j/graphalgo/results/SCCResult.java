package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class SCCResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long setCount;
    public final Long minSetSize;
    public final Long maxSetSize;

    public SCCResult(Long loadMillis,
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends AbstractResultBuilder<SCCResult> {

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
        public SCCResult build() {
            return new SCCResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    setCount,
                    minSetSize,
                    maxSetSize);
        }
    }

}
