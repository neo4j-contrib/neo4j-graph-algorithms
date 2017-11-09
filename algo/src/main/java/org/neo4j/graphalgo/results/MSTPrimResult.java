/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class MSTPrimResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Double weightSum;
    public final Double weightMin;
    public final Double weightMax;
    public final Long relationshipCount;

    public MSTPrimResult(Long loadMillis,
                         Long computeMillis,
                         Long writeMillis,
                         Double weightSum,
                         Double weightMin,
                         Double weightMax,
                         Long relationshipCount) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
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
