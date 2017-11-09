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
package org.neo4j.graphalgo.impl;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainAlgorithm;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  |  /       \  |   (z)
 *  (c)         (h)
 *
 * @author mknblch
 */
public class LouvainTest_398 {

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (z:Node {name:'z'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (c:Node {name:'c'})\n" + // shuffled
                        "CREATE (h:Node {name:'h'})\n" +

                        "CREATE" +
                        
                        " (b)-[:TYPE]->(a),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (e)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(h),\n" +

                        " (b)-[:TYPE]->(e)";


        db = TestDatabaseCreator.createTestDatabase();

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
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        graph = null;
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
        final LouvainAlgorithm louvain = new Louvain(graph, graph, graph, Pools.DEFAULT, 1, 10)
                .compute();
        final int[] communities = louvain.getCommunityIds();
        for (int i = 0; i < communities.length; i++) {
            System.out.println(getName(i) + " : " + communities[i]);
        }
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        assertEquals(3, louvain.getCommunityCount());
    }

    @Test
    public void testParallel() throws Exception {
        final LouvainAlgorithm louvain = new Louvain(graph, graph, graph, Pools.DEFAULT, 8, 10)
                .compute();
        final int[] communities = louvain.getCommunityIds();
        for (int i = 0; i < communities.length; i++) {
            System.out.println(getName(i) + " : " + communities[i]);
        }
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        assertEquals(3, louvain.getCommunityCount());
    }
}
