package org.neo4j.graphalgo.core.heavyweight;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.RandomGraphTestCase;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class HeavyGraphParallelLoadingTest extends RandomGraphTestCase {
    private static final int NODE_COUNT = 100;
    private final int batchSize;

    @Parameters
    public static Collection<Object[]> data() {
        return parameters(
                30,
                1000
        );
    }

    private static Collection<Object[]> parameters(int... batchSizes) {
        return Arrays.stream(batchSizes)
                .mapToObj(b -> new Object[]{b})
                .collect(Collectors.toList());
    }

    private Graph graph;

    public HeavyGraphParallelLoadingTest(int batchSize) {
        this.batchSize = batchSize;
        final ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            graph = new HeavyGraphFactory(db, new GraphSetup(pool))
                    .build(batchSize);
        } catch (Exception e) {
            markFailure();
            throw e;
        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void shouldLoadAllNodes() throws Exception {
        assertEquals(NODE_COUNT, graph.nodeCount());
    }

    @Test
    public void shouldLoadNodesInOrder() throws Exception {
        if (batchSize < NODE_COUNT) {
            graph.forEachNode(nodeId -> {
                assertEquals(
                        nodeId,
                        graph.toOriginalNodeId(nodeId));
                return true;
            });
        } else {
            final Set<Long> nodeIds;
            try (Transaction tx = db.beginTx()) {
                nodeIds = db.getAllNodes().stream()
                        .map(Node::getId)
                        .collect(Collectors.toSet());
                tx.success();
            }

            graph.forEachNode(nodeId -> {
                assertEquals(
                        true,
                        nodeIds.remove(graph.toOriginalNodeId(nodeId)));
                assertEquals(
                        nodeId,
                        graph.toMappedNodeId(graph.toOriginalNodeId(nodeId)));
                return true;
            });
        }
    }

    @Test
    public void shouldLoadAllRelationships() throws Exception {
        try (Transaction tx = db.beginTx()) {
            graph.forEachNode(this::testRelationships);
            tx.success();
        }
    }

    @Test
    public void shouldCollectErrors() throws Exception {
        if (batchSize < NODE_COUNT) {
            String message = "oh noes";
            try {
                new HeavyGraphFactory(
                        db,
                        new GraphSetup(new ThrowingThreadPool(3, message))
                ).build(batchSize);
                fail("Should have thrown an Exception.");
            } catch (Exception e) {
                assertEquals(message, e.getMessage());
                assertEquals(RuntimeException.class, e.getClass());
                final Throwable[] suppressed = e.getSuppressed();
                assertEquals(3, suppressed.length);
                for (Throwable t : suppressed) {
                    assertEquals(message, t.getMessage());
                    assertEquals(RuntimeException.class, t.getClass());
                }
            }
        }
    }

    private boolean testRelationships(int nodeId) {
        testRelationships(nodeId, Direction.OUTGOING);
        testRelationships(nodeId, Direction.INCOMING);
        return true;
    }

    private void testRelationships(int nodeId, final Direction direction) {
        final Node node = db.getNodeById(graph.toOriginalNodeId(nodeId));
        final Map<Long, Relationship> relationships = Iterables
                .stream(node.getRelationships(direction))
                .collect(Collectors.toMap(
                        rel -> RawValues.combineIntInt((int) rel.getStartNode().getId(), (int) rel.getEndNode().getId()),
                        Function.identity()));
        graph.forEachRelationship(
                nodeId,
                direction,
                (sourceId, targetId, relationId) -> {
                    assertEquals(nodeId, sourceId);
                    final Relationship relationship = relationships.remove(
                            relationId);
                    assertNotNull(
                            "Relation that does not exist in the graph",
                            relationship);

                    if (direction == Direction.OUTGOING) {
                        assertEquals(
                                relationship.getStartNode().getId(),
                                graph.toOriginalNodeId(sourceId));
                        assertEquals(
                                relationship.getEndNode().getId(),
                                graph.toOriginalNodeId(targetId));
                    } else {
                        assertEquals(
                                relationship.getEndNode().getId(),
                                graph.toOriginalNodeId(sourceId));
                        assertEquals(
                                relationship.getStartNode().getId(),
                                graph.toOriginalNodeId(targetId));
                    }
                    return true;
                });

        assertTrue(
                "Relationships that were not traversed " + relationships,
                relationships.isEmpty());
    }

    private static class ThrowingThreadPool extends ThreadPoolExecutor {
        private final String message;

        private ThrowingThreadPool(int numberOfThreads, String message) {
            super(
                    numberOfThreads,
                    numberOfThreads,
                    1,
                    TimeUnit.MINUTES,
                    new LinkedBlockingDeque<>());
            this.message = message;
        }

        @Override
        public Future<?> submit(final Runnable task) {
            final CompletableFuture<Object> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException(message));
            return future;
        }
    }
}
