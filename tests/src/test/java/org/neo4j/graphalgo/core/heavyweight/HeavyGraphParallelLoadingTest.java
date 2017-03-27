package org.neo4j.graphalgo.core.heavyweight;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.PrintWriter;
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
public class HeavyGraphParallelLoadingTest {
    private static final int NODE_COUNT = 100;
    private static boolean hasFailures = false;
    private final int batchSize;

    @Rule
    public TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(
                final Throwable e,
                final Description description) {
            hasFailures = true;
        }
    };

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

    private static final String RANDOM_GRAPH =
            "FOREACH (x IN range(1, 100) | CREATE ()) " +
                    "WITH 0.1 AS p " +
                    "MATCH (n1),(n2) WITH n1,n2 LIMIT 1000 WHERE rand() < p " +
                    "CREATE (n1)-[:TYPE]->(n2)";


    private static GraphDatabaseService db;
    private Graph graph;

    public HeavyGraphParallelLoadingTest(int batchSize) {
        this.batchSize = batchSize;
        final ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            graph = new HeavyGraphFactory(
                    (GraphDatabaseAPI) db,
                    null,
                    null,
                    null,
                    pool
            ).build(batchSize);
        } catch (Exception e) {
            hasFailures = true;
            throw e;
        } finally {
            pool.shutdown();
        }
    }

    @BeforeClass
    public static void setupGraph() {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(RANDOM_GRAPH).close();
            tx.success();
        } catch (Exception e) {
            hasFailures = true;
            throw e;
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (hasFailures) {
            PrintWriter pw = new PrintWriter(System.out);
            pw.println("Generated graph to reproduce any errors:");
            pw.println();
            CypherExporter.export(pw, db);
        }
        db.shutdown();
    }

    @Test
    public void shouldLoadAllNodes() throws Exception {
        assertEquals(NODE_COUNT, graph.nodeCount());
    }

    @Test
    public void shouldLoadNodesInOrder() throws Exception {
        if (batchSize < NODE_COUNT) {
            graph.forEachNode(nodeId -> assertEquals(
                    nodeId,
                    graph.toOriginalNodeId(nodeId)));
        } else {
            final Set<Long> nodeIds;
            try (Transaction tx = db.beginTx()) {
                nodeIds = db.getAllNodes().stream()
                        .map(Node::getId)
                        .collect(Collectors.toSet());
                tx.success();
            }

            graph.forEachNode(nodeId -> {
                assertEquals(true, nodeIds.remove(graph.toOriginalNodeId(nodeId)));
                assertEquals(nodeId, graph.toMappedNodeId(graph.toOriginalNodeId(nodeId)));
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
                        (GraphDatabaseAPI) db,
                        null,
                        null,
                        null,
                        new ThrowingThreadPool(3, message)
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

    private void testRelationships(int nodeId) {
        testRelationships(nodeId, Direction.OUTGOING);
        testRelationships(nodeId, Direction.INCOMING);
    }

    private void testRelationships(int nodeId, final Direction direction) {
        final Node node = db.getNodeById(graph.toOriginalNodeId(nodeId));
        final Map<Long, Relationship> relationships = Iterables
                .stream(node.getRelationships(direction))
                .collect(Collectors.toMap(
                        Relationship::getId,
                        Function.identity()));
        graph.forEachRelation(
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
