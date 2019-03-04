package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.function.Supplier;

public enum DuplicateRelationshipsStrategy {
    NONE {
        public double merge(double runningTotal, double weight) {
            throw new UnsupportedOperationException();
        }
    },
    SKIP {
        public double merge(double runningTotal, double weight) {
            return runningTotal;
        }
    },
    SUM {
        public double merge(double runningTotal, double weight) {
            return runningTotal + weight;
        }
    },
    MIN {
        public double merge(double runningTotal, double weight) {
            return Math.min(runningTotal, weight);
        }
    },
    MAX {
        public double merge(double runningTotal, double weight) {
            return Math.max(runningTotal, weight);
        }
    };

    public abstract double merge(double runningTotal, double weight);

    public void handle(int source, int target, AdjacencyMatrix matrix, boolean hasRelationshipWeights, WeightMap relWeights, Supplier<Number> weightSupplier) {
        if (this == DuplicateRelationshipsStrategy.NONE) {
            matrix.addOutgoing(source, target);
            if (hasRelationshipWeights) {
                long relId = RawValues.combineIntInt(source, target);
                Number weight = weightSupplier.get();
                if (weight != null) {
                    relWeights.put(relId, weight.doubleValue());
                }
            }
        } else {
            boolean hasRelationship = matrix.hasOutgoing(source, target);

            if (!hasRelationship) {
                matrix.addOutgoing(source, target);
            }

            if (hasRelationshipWeights) {
                long relationship = RawValues.combineIntInt(source, target);

                double oldWeight = relWeights.get(relationship, 0d);
                Number weight = weightSupplier.get();

                if (weight != null) {
                    double thisWeight = weight.doubleValue();
                    double newWeight = hasRelationship ? this.merge(oldWeight, thisWeight) : thisWeight;
                    relWeights.put(relationship, newWeight);
                }
            }
        }
    }
}
