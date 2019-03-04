package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;

import java.util.Map;

public class Nodes {
    private final long offset;
    private final long rows;
    IdMap idMap;
    WeightMap nodeWeights;
    WeightMap nodeProps;
    private final Map<PropertyMapping, WeightMap> nodeProperties;
    private final double defaultNodeWeight;
    private final double defaultNodeValue;

    Nodes(
            long offset,
            long rows,
            IdMap idMap,
            WeightMap nodeWeights,
            WeightMap nodeProps,
            Map<PropertyMapping, WeightMap> nodeProperties,
            double defaultNodeWeight,
            double defaultNodeValue) {
        this.offset = offset;
        this.rows = rows;
        this.idMap = idMap;
        this.nodeWeights = nodeWeights;
        this.nodeProps = nodeProps;
        this.nodeProperties = nodeProperties;
        this.defaultNodeWeight = defaultNodeWeight;
        this.defaultNodeValue = defaultNodeValue;
    }

    public Map<PropertyMapping, WeightMap> nodeProperties() {
        return nodeProperties;
    }

    private WeightMapping nodeWeights() {
        if (nodeWeights != null) {
            return nodeWeights;
        }
        return new NullWeightMap(defaultNodeValue);
    }

    private WeightMapping nodeValues() {
        if (nodeProps != null) {
            return nodeProps;
        }
        return new NullWeightMap(defaultNodeWeight);
    }

    public long offset() {
        return offset;
    }

    long rows() {
        return rows;
    }
}
