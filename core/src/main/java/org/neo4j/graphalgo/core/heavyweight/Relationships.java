package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;

class Relationships {
    private final long offset;
    private final long rows;
    private final AdjacencyMatrix matrix;
    private final WeightMap relWeights;
    private final double defaultWeight;

    Relationships(long offset, long rows, AdjacencyMatrix matrix, WeightMap relWeights, double defaultWeight) {
        this.offset = offset;
        this.rows = rows;
        this.matrix = matrix;
        this.relWeights = relWeights;
        this.defaultWeight = defaultWeight;
    }

    WeightMapping weights() {
        if (relWeights != null) {
            return relWeights;
        }
        return new NullWeightMap(defaultWeight);
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }

    public long rows() {
        return rows;
    }

    public long offset() {
        return offset;
    }

    public WeightMap relWeights() {
        return relWeights;
    }
}
