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

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author mknblch
 */
public class HeavyCypherGraphFactory extends GraphFactory {

    private static final int NO_BATCH = -1;
    private static final int INITIAL_NODE_COUNT = 1_000_000;
    private static final int ESTIMATED_DEGREE = 3;
    private static final String LIMIT = "limit";
    private static final String SKIP = "skip";
    public static final String TYPE = "cypher";

    public HeavyCypherGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    static class Nodes {
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
    }

    static class Relationships {

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

        private WeightMapping weights() {
            if (relWeights != null) {
                return relWeights;
            }
            return new NullWeightMap(defaultWeight);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public Graph build() {
        int batchSize = setup.batchSize;

        Nodes nodes = canBatchLoad(batchSize, setup.startLabel) ?
                batchLoadNodes(batchSize) :
                loadNodes(0, NO_BATCH);
        Relationships relationships;
        relationships = canBatchLoad(batchSize, setup.relationshipType) ?
                batchLoadRelationships(batchSize, nodes) :
                loadRelationships(0, NO_BATCH, nodes);

        if (setup.sort) {
            relationships.matrix.sortAll(setup.executor, setup.concurrency);
        }

        Map<String, WeightMapping> nodePropertyMappings = new HashMap<>();
        for (Map.Entry<PropertyMapping, WeightMap> entry : nodes.nodeProperties.entrySet()) {
            nodePropertyMappings.put(entry.getKey().propertyName, entry.getValue());
        }

        return new HeavyGraph(
                nodes.idMap,
                relationships.matrix,
                relationships.weights(),
                nodePropertyMappings);
    }

    private Relationships batchLoadRelationships(int batchSize, Nodes nodes) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();
        boolean accumulateWeights = setup.accumulateWeights;

        // data structures for merged information
        int nodeCount = nodes.idMap.size();
        AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);
        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(
                hasRelationshipWeights,
                setup.relationDefaultWeight,
                nodeCount * ESTIMATED_DEGREE);

        long offset = 0;
        long lastOffset = 0;
        long total = 0;
        List<Future<Relationships>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            // suboptimal, each sub-call allocates a AdjacencyMatrix of nodeCount size, would be better with a sparse variant
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize, nodes)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Relationships> future : futures) {
                    Relationships result = get(
                            "Error during loading relationships offset: " + (lastOffset + batchSize),
                            future);
                    lastOffset = result.offset;
                    total += result.rows;
                    working = result.rows > 0;
                    if (working) {
                        WeightMap resultWeights = hasRelationshipWeights && result.relWeights.size() > 0 ? result.relWeights : null;
                        result.matrix.nodesWithRelationships(Direction.OUTGOING).forEachNode(
                                node -> {
                                    result.matrix.forEach(node, Direction.OUTGOING,
                                            (source, target, relationship) -> {
                                                if (accumulateWeights) {
                                                    // suboptimial, O(n) per node
                                                    if (!matrix.hasOutgoing(source, target)) {
                                                        matrix.addOutgoing(source, target);
                                                    }
                                                    if (resultWeights != null) {
                                                        double oldWeight = relWeights.get(relationship, 0d);
                                                        double newWeight = resultWeights.get(relationship) + oldWeight;
                                                        relWeights.put(relationship, newWeight);
                                                    }
                                                } else {
                                                    matrix.addOutgoing(source, target);
                                                    if (resultWeights != null) {
                                                        relWeights.put(relationship, resultWeights.get(relationship));
                                                    }
                                                }
                                                return true;
                                            });
                                    return true;
                                });
                    }
                }
                futures.clear();
            }
        } while (working);

        return new Relationships(0, total, matrix, relWeights, setup.relationDefaultWeight);
    }

    private Nodes batchLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        // data structures for merged information
        int capacity = INITIAL_NODE_COUNT * 10;
        LongIntHashMap nodeToGraphIds = new LongIntHashMap(capacity);

        Map<PropertyMapping, WeightMap> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            nodeProperties.put(propertyMapping,
                    newWeightMapping(true, dimensions.nodePropertyDefaultValue(propertyMapping.propertyName), capacity));
        }

        long offset = 0;
        long total = 0;
        long lastOffset = 0;
        List<Future<Nodes>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadNodes(skip, batchSize)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Nodes> future : futures) {
                    Nodes result = get("Error during loading nodes offset: " + (lastOffset + batchSize), future);
                    lastOffset = result.offset;
                    total += result.rows;
                    working = result.idMap.size() > 0;
                    if (working) {
                        int minNodeId = nodeToGraphIds.size();

                        result.idMap.nodeToGraphIds().forEach(
                                (LongIntProcedure) (graphId, algoId) -> {
                                    int newId = algoId + minNodeId;
                                    nodeToGraphIds.put(graphId, newId);

                                    for (Map.Entry<PropertyMapping, WeightMap> entry : nodeProperties.entrySet()) {
                                        entry.getValue().put(
                                                newId,
                                                result.nodeProperties.get(entry.getKey()).get(algoId));
                                    }
                                });
                    }
                }
                futures.clear();
            }
        } while (working);

        long[] graphIds = new long[nodeToGraphIds.size()];
        for (final LongIntCursor cursor : nodeToGraphIds) {
            graphIds[cursor.value] = cursor.key;
        }
        return new Nodes(
                0L,
                total,
                new IdMap(graphIds,nodeToGraphIds),
                null,null,
                nodeProperties,
                setup.nodeDefaultWeight,
                setup.nodeDefaultPropertyValue
        );
    }

    private <T> T get(String message, Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted: " + message, e);
        } catch (ExecutionException e) {
            throw new RuntimeException(message, e);
        }
    }

    private boolean canBatchLoad(int batchSize, String statement) {
        return setup.loadConcurrent() && batchSize > 0 &&
                (statement.contains("{" + LIMIT + "}") || statement.contains("$" + LIMIT)) &&
                (statement.contains("{" + SKIP + "}") || statement.contains("$" + SKIP));
    }

    private Relationships loadRelationships(long offset, int batchSize, Nodes nodes) {

        IdMap idMap = nodes.idMap;

        int nodeCount = idMap.size();
        int capacity = batchSize == NO_BATCH ? nodeCount : batchSize;

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);

        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, capacity);

        class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
            private long lastSourceId = -1, lastTargetId = -1;
            private int source = -1, target = -1;
            private long rows = 0;

            @Override
            public boolean visit(Result.ResultRow row) throws RuntimeException {
                rows++;
                long sourceId = row.getNumber("source").longValue();
                if (sourceId != lastSourceId) {
                    source = idMap.get(sourceId);
                    lastSourceId = sourceId;
                }
                if (source == -1) {
                    return true;
                }
                long targetId = row.getNumber("target").longValue();
                if (targetId != lastTargetId) {
                    target = idMap.get(targetId);
                    lastTargetId = targetId;
                }
                if (target == -1) {
                    return true;
                }
                if (hasRelationshipWeights) {
                    long relId = RawValues.combineIntInt(source, target);
                    Object weight = getProperty(row, "weight");
                    if (weight instanceof Number) {
                        relWeights.put(relId, ((Number) weight).doubleValue());
                    }
                }
                matrix.addOutgoing(source, target);
                return true;
            }
        }
        RelationshipRowVisitor visitor = new RelationshipRowVisitor();
        api.execute(setup.relationshipType, params(offset, batchSize)).accept(visitor);
        return new Relationships(offset, visitor.rows, matrix, relWeights, setup.relationDefaultWeight);
    }

    private Nodes loadNodes(long offset, int batchSize) {
        int capacity = batchSize == NO_BATCH ? INITIAL_NODE_COUNT : batchSize;
        final IdMap idMap = new IdMap(capacity);

        Map<PropertyMapping, WeightMap> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            nodeProperties.put(propertyMapping,
                    newWeightMapping(true, dimensions.nodePropertyDefaultValue(propertyMapping.propertyName), capacity));
        }

        class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
            private long rows;

            @Override
            public boolean visit(Result.ResultRow row) throws RuntimeException {
                rows++;
                long id = row.getNumber("id").longValue();
                idMap.add(id);

                for (Map.Entry<PropertyMapping, WeightMap> entry : nodeProperties.entrySet()) {
                    Object value = getProperty(row, entry.getKey().propertyKey);
                    if (value instanceof Number) {
                        entry.getValue().put(id, ((Number) value).doubleValue());
                    }
                }

                return true;
            }
        }

        NodeRowVisitor visitor = new NodeRowVisitor();
        api.execute(setup.startLabel, params(offset, batchSize)).accept(visitor);
        idMap.buildMappedIds();
        return new Nodes(
                offset,
                visitor.rows,
                idMap,
                null,
                null,
                nodeProperties,
                setup.nodeDefaultWeight,
                setup.nodeDefaultPropertyValue);
    }

    private Object getProperty(Result.ResultRow row, String propertyName) {
        try {
            return row.get(propertyName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private WeightMap newWeightMapping(boolean needWeights, double defaultValue, int capacity) {
        return needWeights ? new WeightMap(capacity, defaultValue, -2) : null;
    }

    private Map<String, Object> params(long offset, int batchSize) {
        Map<String, Object> params = new HashMap<>(setup.params);
        params.put(SKIP, offset);
        if (batchSize > 0) {
            params.put(LIMIT, batchSize);
        }
        return params;
    }
}
