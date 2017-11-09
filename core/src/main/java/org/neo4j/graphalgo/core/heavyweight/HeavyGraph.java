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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;

/**
 * Heavy weighted graph built of an adjacency matrix.
 *
 * @author mknblch
 */
public class HeavyGraph implements Graph, NodeWeights, NodeProperties {

    private final IdMap nodeIdMap;
    private AdjacencyMatrix container;
    private WeightMapping relationshipWeights;
    private WeightMapping nodeWeights;
    private WeightMapping nodeProperties;

    HeavyGraph(
            IdMap nodeIdMap,
            AdjacencyMatrix container,
            final WeightMapping relationshipWeights,
            final WeightMapping nodeWeights,
            final WeightMapping nodeProperties) {
        this.nodeIdMap = nodeIdMap;
        this.container = container;
        this.relationshipWeights = relationshipWeights;
        this.nodeWeights = nodeWeights;
        this.nodeProperties = nodeProperties;
    }

    @Override
    public long nodeCount() {
        return nodeIdMap.size();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        nodeIdMap.forEach(consumer);
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return nodeIdMap.iterator();
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return nodeIdMap.batchIterables(batchSize);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        return container.degree(nodeId, direction);
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        container.forEach(nodeId, direction, consumer);
    }

    @Override
    public void forEachRelationship(
            final int nodeId,
            final Direction direction,
            final WeightedRelationshipConsumer consumer) {
        container.forEach(nodeId, direction, relationshipWeights, consumer);
    }

    @Override
    public int toMappedNodeId(long originalNodeId) {
        return nodeIdMap.get(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(int mappedNodeId) {
        return nodeIdMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeIdMap.contains(nodeId);
    }

    @Override
    public double weightOf(final int sourceNodeId, final int targetNodeId) {
        long relId = container.isBoth
                ? RawValues.combineSorted(sourceNodeId, targetNodeId)
                : RawValues.combineIntInt(sourceNodeId, targetNodeId);
        return relationshipWeights.get(relId);
    }

    @Override
    public double weightOf(final int nodeId) {
        return nodeWeights.get(nodeId);
    }

    @Override
    public double valueOf(final int nodeId, final double defaultValue) {
        return nodeProperties.get(nodeId, defaultValue);
    }

    @Override
    public void release() {
        container = null;
        relationshipWeights = null;
        nodeWeights = null;
        nodeProperties = null;
    }
}
