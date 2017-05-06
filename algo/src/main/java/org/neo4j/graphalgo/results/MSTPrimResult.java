package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class MSTPrimResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Double weightSum;
    public final Double weightMin;
    public final Double weightMax;
    public final Long relationshipCount;

    public MSTPrimResult(Long loadDuration,
                         Long evalDuration,
                         Long writeDuration,
                         Double weightSum,
                         Double weightMin,
                         Double weightMax,
                         Long relationshipCount) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.weightSum = weightSum;
        this.weightMin = weightMin;
        this.weightMax = weightMax;
        this.relationshipCount = relationshipCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<MSTPrimResult> {

        protected double weightSum = 0.0;
        protected double weightMin = 0.0;
        protected double weightMax = 0.0;
        protected long relationshipCount = 0;

        public Builder withWeightSum(double weightSum) {
            this.weightSum = weightSum;
            return this;
        }

        public Builder withWeightMin(double weightMin) {
            this.weightMin = weightMin;
            return this;
        }

        public Builder withWeightMax(double weightMax) {
            this.weightMax = weightMax;
            return this;
        }

        public Builder withRelationshipCount(long count) {
            this.relationshipCount = count;
            return this;
        }

        public MSTPrimResult build() {
            return new MSTPrimResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    weightSum,
                    weightMin,
                    weightMax,
                    relationshipCount);
        }
    }
}
