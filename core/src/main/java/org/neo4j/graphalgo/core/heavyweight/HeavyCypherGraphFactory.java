package org.neo4j.graphalgo.core.heavyweight;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
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

    public HeavyCypherGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api,setup);
    }

    static class Nodes {
        private final long offset;
        private final long rows;
        IdMap idMap;
        WeightMapping nodeWeights;
        WeightMapping nodeProps;

        Nodes(long offset, long rows, IdMap idMap, WeightMapping nodeWeights, WeightMapping nodeProps) {
            this.offset = offset;
            this.rows = rows;
            this.idMap = idMap;
            this.nodeWeights = nodeWeights;
            this.nodeProps = nodeProps;
        }
    }

    static class Relationships {

        private final long offset;
        private final long rows;
        private final AdjacencyMatrix matrix;
        private final WeightMapping relWeights;

        Relationships(long offset, long rows, AdjacencyMatrix matrix, WeightMapping relWeights) {
            this.offset = offset;
            this.rows = rows;
            this.matrix = matrix;
            this.relWeights = relWeights;
        }
    }
    @SuppressWarnings("WeakerAccess")
    public Graph build() {
        int batchSize = setup.batchSize;

        Nodes nodes = canBatchLoad(batchSize, setup.nodeStatement) ?
                batchLoadNodes(batchSize) :
                loadNodes(0, NO_BATCH);
        Relationships relationships;
        relationships = canBatchLoad(batchSize, setup.relationshipStatement) ?
                batchLoadRelationships(batchSize, nodes) :
                loadRelationships(0, NO_BATCH, nodes);

        return new HeavyGraph(nodes.idMap, relationships.matrix, relationships.relWeights, nodes.nodeWeights, nodes.nodeProps);
    }

    private Relationships batchLoadRelationships(int batchSize, Nodes nodes) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();
        boolean accumulateWeights = setup.accumulateWeights;

        // data structures for merged information
        int nodeCount = nodes.idMap.size();
        AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);
        boolean hasRelationshipWeights = !setup.loadDefaultRelationshipWeight();
        final WeightMapping relWeights = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, nodeCount*ESTIMATED_DEGREE);

        long offset = 0;
        long lastOffset = 0;
        long total = 0;
        List<Future<Relationships>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            // suboptimal, each sub-call allocates a AdjacencyMatrix of nodeCount size, would be better with a sparse variant
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize,nodes)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Relationships> future : futures) {
                    Relationships result = get("Error during loading relationships offset: "+(lastOffset+batchSize),future);
                    lastOffset = result.offset;
                    total += result.rows;
                    working = result.rows > 0;
                    if (working) {
                        WeightMapping resultWeights = hasRelationshipWeights && result.relWeights.size() > 0 ? result.relWeights : null;
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
                                                        relWeights.set(relationship,
                                                                resultWeights.get(relationship) +
                                                                        relWeights.get(relationship, 0d));
                                                    }
                                                } else {
                                                    matrix.addOutgoing(source, target);
                                                    if (resultWeights != null) {
                                                        relWeights.set(relationship, resultWeights.get(relationship));
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

        return new Relationships(0, total, matrix,relWeights);
    }

    private Nodes batchLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        // data structures for merged information
        int capacity = INITIAL_NODE_COUNT * 10;
        LongIntMap nodeToGraphIds = new LongIntHashMap(capacity);

        boolean hasNodeWeights = !setup.loadDefaultNodeWeight();
        final WeightMapping nodeWeights = newWeightMapping(hasNodeWeights, setup.nodeDefaultWeight, capacity);

        boolean hasNodeProperty = !setup.loadDefaultNodeProperty();
        final WeightMapping nodeProps = newWeightMapping(hasNodeProperty, setup.nodeDefaultPropertyValue, capacity);

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
                    Nodes result = get("Error during loading nodes offset: "+(lastOffset+batchSize),future);
                    lastOffset = result.offset;
                    total += result.rows;
                    working = result.idMap.size() > 0;
                    if (working) {
                        int minNodeId = nodeToGraphIds.size();
                        WeightMapping resultWeights = hasNodeWeights && result.nodeWeights.size() > 0 ? result.nodeWeights : null;
                        WeightMapping resultProps = hasNodeProperty && result.nodeProps.size() > 0 ? result.nodeProps : null;
                        result.idMap.nodeToGraphIds().forEach(
                                (LongIntProcedure)(graphId,algoId) -> {
                                    int newId = algoId + minNodeId;
                                    nodeToGraphIds.put(graphId, newId);
                                    if (resultWeights!=null) {
                                        nodeWeights.set(newId, resultWeights.get(algoId));
                                    }
                                    if (resultProps != null) {
                                        nodeProps.set(newId, resultProps.get(algoId));
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
        return new Nodes(0,total, new IdMap(graphIds,nodeToGraphIds),nodeWeights,nodeProps);
    }

    private <T> T get(String message, Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted: " + message,e);
        } catch (ExecutionException e) {
            throw new RuntimeException(message,e);
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

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);

        boolean hasRelationshipWeights = !setup.loadDefaultRelationshipWeight();
        final WeightMapping relWeigths = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, capacity);

        class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
            private long lastSourceId = -1, lastTargetId = -1;
            private int source = -1, target = -1;
            private long rows=0;

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
                    relWeigths.set(relId, row.get("weight"));
                }
                matrix.addOutgoing(source, target);
                return true;
            }
        }
        RelationshipRowVisitor visitor = new RelationshipRowVisitor();
        api.execute(setup.relationshipStatement, params(offset, batchSize)).accept(visitor);
        return new Relationships(offset, visitor.rows, matrix, relWeigths);
    }

    private Nodes loadNodes(long offset, int batchSize) {
        int capacity = batchSize == NO_BATCH ? INITIAL_NODE_COUNT : batchSize;
        final IdMap idMap = new IdMap(capacity);

        boolean hasNodeWeights = !setup.loadDefaultNodeWeight();
        final WeightMapping nodeWeights = newWeightMapping(hasNodeWeights, setup.nodeDefaultWeight, capacity);

        boolean hasNodeProperty = !setup.loadDefaultNodeProperty();
        final WeightMapping nodeProps = newWeightMapping(hasNodeProperty, setup.nodeDefaultPropertyValue, capacity);

        class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
            private long rows;
            @Override
            public boolean visit(Result.ResultRow row) throws RuntimeException {
                rows++;
                long id = row.getNumber("id").longValue();
                idMap.add(id);
                if (hasNodeWeights) {
                    nodeWeights.set(id, row.get("weight"));
                }
                if (hasNodeProperty) {
                    nodeProps.set(id, row.get("value"));
                }
                return true;
            }
        }

        NodeRowVisitor visitor = new NodeRowVisitor();
        api.execute(setup.nodeStatement, params(offset, batchSize)).accept(visitor);
        return new Nodes(offset, visitor.rows, idMap, nodeWeights, nodeProps);
    }

    private WeightMapping newWeightMapping(boolean needWeights, double defaultValue, int capacity) {
        return needWeights ?
                new WeightMap(capacity, defaultValue) :
                new NullWeightMap(defaultValue);
    }

    private Map<String, Object> params(long offset, int batchSize) {
        return batchSize > 0 ? MapUtil.map(SKIP, offset, LIMIT, batchSize) : MapUtil.map(SKIP, offset);
    }
}
