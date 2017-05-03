package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class SCCResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Long setCount;
    public final Long minSetSize;
    public final Long maxSetSize;

    public SCCResult(Long loadDuration,
                     Long evalDuration,
                     Long writeDuration,
                     Long setCount,
                     Long minSetSize,
                     Long maxSetSize) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
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
