package org.neo4j.graphalgo.core.heavyweight;

import algo.Pools;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class HeavyCypherGraphParallelFactoryTest {

    private static final int COUNT = 10000;
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() {

        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Iterators.count(db.execute("UNWIND range(1," + COUNT + ") AS id CREATE (n {id:id})-[:REL {prop:id%10}]->(n)"));
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testLoadNoneParallelCypher() throws Exception {
        String nodeStatement = "MATCH (n) RETURN id(n) as id";
        String relStatement = "MATCH (n)-[r:REL]->(m) RETURN id(n) as source, id(m) as target, r.prop as weight";

        loadAndTestGraph(nodeStatement, relStatement, false);
    }
    @Test
    public void testLoadNodesParallelCypher() throws Exception {
        String nodeStatement = "MATCH (n) WITH n SKIP {skip} LIMIT {limit} RETURN id(n) as id";
        String relStatement = "MATCH (n)-[r:REL]->(m) RETURN id(n) as source, id(m) as target, r.prop as weight";

        loadAndTestGraph(nodeStatement, relStatement, false);
    }

    @Test
    public void testLoadRelationshipsParallelCypher() throws Exception {
        String nodeStatement = "MATCH (n) RETURN id(n) as id";
        String relStatement = "MATCH (n)-[r:REL]->(m)  WITH * SKIP {skip} LIMIT {limit} RETURN id(n) as source, id(m) as target, r.prop as weight";

        loadAndTestGraph(nodeStatement, relStatement, false);
    }

    @Test
    public void testLoadRelationshipsParallelAccumulateWeightCypher() throws Exception {
        String nodeStatement = "MATCH (n) RETURN id(n) as id";
        String relStatement =
                "MATCH (n)-[r:REL]->(m) WITH * SKIP {skip} LIMIT {limit} RETURN id(n) as source, id(m) as target, r.prop/2.0 as weight " +
                "UNION ALL "+
                "MATCH (n)-[r:REL]->(m) WITH * SKIP {skip} LIMIT {limit} RETURN id(n) as source, id(m) as target, r.prop/2.0 as weight ";

        loadAndTestGraph(nodeStatement, relStatement, true);
    }

    @Test
    public void testLoadCypherBothParallel() throws Exception {
        String nodeStatement = "MATCH (n) WITH n SKIP {skip} LIMIT {limit} RETURN id(n) as id";
        String relStatement = "MATCH (n)-[r:REL]->(m) WITH * SKIP {skip} LIMIT {limit} RETURN id(n) as source, id(m) as target, r.prop as weight";

        loadAndTestGraph(nodeStatement, relStatement, false);
    }

    protected void loadAndTestGraph(String nodeStatement, String relStatement, boolean accumulateWeights) {
        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withExecutorService(Pools.DEFAULT)
                .withBatchSize(1000)
                .withAccumulateWeights(accumulateWeights)
                .withRelationshipWeightsFromProperty("prop",0d)
                .withNodeStatement(nodeStatement)
                .withRelationshipStatement(relStatement)
                .load(HeavyCypherGraphFactory.class);

        Assert.assertEquals(COUNT, graph.nodeCount());
        AtomicInteger relCount = new AtomicInteger();
        graph.forEachNode(node -> {relCount.addAndGet(graph.degree(node, Direction.OUTGOING));return true;});
        Assert.assertEquals(COUNT, relCount.get());
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.weightedRelationshipIterator(n, Direction.OUTGOING).forEachRemaining(rel -> total.addAndGet((int) rel.weight));
            return true;
        });
        assertEquals(9*COUNT/2,total.get());
    }
}
