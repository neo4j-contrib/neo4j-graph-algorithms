package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.function.IntConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
public class LouvainClusteringIntegrationTest {

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (c:Node {name:'c'})\n" + // shuffled
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (z:Node {name:'z'})\n" +

                        "CREATE" +
                        
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(d),\n" +
                        
                        " (e)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(g),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(g),\n" +
                        
                        " (b)-[:TYPE {w:100}]->(e)";


        db = TestDatabaseCreator.createTestDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LouvainProc.class);

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("w", 0.0)
                .load(HeavyGraphFactory.class);

    }

    @AfterClass
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    private String getName(long nodeId) {
        String[] name = {""};
        try (Transaction tx = db.beginTx()) {
            db.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
                name[0] = (String) row.getNode("n").getProperty("name");
                return true;
            });
        }
        return name[0];
    }

    @Test
    public void test() throws Exception {
        final String cypher = "CALL algo.louvain('', '', {write:true, writeProperty:'community', concurrency:2}) " +
                "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        db.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long iterations = row.getNumber("iterations").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community cound", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }


    @Test
    public void testStream() throws Exception {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:2}) " +
                "YIELD nodeId, community";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        db.execute(cypher).accept(row -> {
            testMap.addTo(row.getNumber("community").intValue(), 1);
            return false;
        });
        assertEquals(3, testMap.size());
    }

    @Test
    public void testWithLabelRel() throws Exception {
        final String cypher = "CALL algo.louvain('Node', 'TYPE', {write:true, writeProperty:'community', concurrency:2}) " +
                "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        db.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long iterations = row.getNumber("iterations").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community cound", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }

    // weightProperty is not
    @Ignore("TODO")
    @Test
    public void testWithWeight() throws Exception {
        final String cypher = "CALL algo.louvain('Node', 'TYPE', {weightProperty:'w', defaultValue:2.0, " +
                "write:true, writeProperty:'cluster', concurrency:2}) " +
                "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        db.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long iterations = row.getNumber("iterations").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community cound", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }

}
