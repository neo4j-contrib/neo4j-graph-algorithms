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
package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.AfterClass;
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

    private final static String[] NODES = {"a", "b", "c", "d", "e", "f", "g", "h", "z"};


    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" + // shuffled
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (z:Node {name:'z'})\n" +

                        "CREATE" +
                        
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(d),\n" +
                        
                        " (f)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(g),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(g),\n" +
                        " (g)-[:TYPE]->(h),\n" +

                        // makes b & e more likely to build their own set
                        " (b)-[:TYPE]->(e)";

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
                .withRelationshipWeightsFromProperty("w", 1.0)
                .load(HeavyGraphFactory.class);

    }

    @AfterClass
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    private String getName(long nodeId) {
        String[] name = {""};
        db.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
            name[0] = (String) row.getNode("n").getProperty("name");
            return true;
        });
//        try (Transaction tx = db.beginTx()) {
//        }
        return name[0];
    }

    @Test
    public void test() throws Exception {
        final String cypher = "CALL algo.louvain('', '') " +
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
            assertEquals("wrong community count", 3, communityCount);
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
        final String cypher = "CALL algo.louvain('Node', 'TYPE') " +
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
            assertEquals("wrong community count", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });

        printNodeSets();
    }

    @Ignore("Issue #444")
    @Test
    public void testWithWeight() throws Exception {
        final String cypher = "CALL algo.louvain('Node', 'TYPE', {weightProperty:'w', defaultValue:1.0}) " +
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
            assertEquals(4, communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count", 9, nodes);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });

        printNodeSets();

    }

    public void printNodeSets() {
        final StringBuilder builder = new StringBuilder();
        for (String node : NODES) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(String.format("%s : C%d", node, getClusterId(node)));
        }
        System.out.println(builder.toString());
    }


    public int getClusterId(String nodeName) {

        int id[] = {0};
        db.execute("MATCH (n) WHERE n.name = '" + nodeName + "' RETURN n").accept(row -> {
            id[0] = (int) row.getNode("n").getProperty("community");
            return true;
        });
        return id[0];
    }

}
