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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TriangleProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *      (a)--(b)-(d)--(e)
 *        \T1/ \   \T2/
 *        (c)  (g)-(f)
 *          \  /T3\
 *           (h)-(i)
 *
 * @author mknblch
 */
public class TriangleProcTest {

    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (c)-[:TYPE]->(h),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(f),\n" +
                        " (f)-[:TYPE]->(d),\n" +

                        " (b)-[:TYPE]->(d),\n" +

                        " (g)-[:TYPE]->(h),\n" +
                        " (h)-[:TYPE]->(i),\n" +
                        " (i)-[:TYPE]->(g)";

        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(TriangleProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void testTriangleCountWriteCypher() throws Exception {
        final String cypher = "CALL algo.triangleCount('Node', '', {concurrency:4, write:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount";
        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
            return true;
        });

        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
        api.execute(request).accept(row -> {
            final int triangles = row.getNumber("t").intValue();
            assertEquals(1, triangles);
            return true;
        });
    }


    @Test
    public void testTriangleCountExp1WriteCypher() throws Exception {
        final String cypher = "CALL algo.triangleCount.exp1('Node', '', {concurrency:4, write:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount";
        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
            return true;
        });

        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
        api.execute(request).accept(row -> {
            final int triangles = row.getNumber("t").intValue();
            assertEquals(1, triangles);
            return true;
        });
    }

    @Test
    public void testTriangleCountStream() throws Exception {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
            return true;
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    public void testTriangleCountExp1Stream() throws Exception {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.exp1.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
            return true;
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    public void testTriangleStream() throws Exception {
        final TripleConsumer mock = mock(TripleConsumer.class);
        final String cypher = "CALL algo.triangle.stream('Node', '', {concurrency:4}) YIELD nodeA, nodeB, nodeC";
        api.execute(cypher).accept(row -> {
            final long nodeA = row.getNumber("nodeA").longValue();
            final long nodeB = row.getNumber("nodeB").longValue();
            final long nodeC = row.getNumber("nodeC").longValue();
            mock.consume(nodeA, nodeB, nodeC);
            return true;
        });

        verify(mock, times(1)).consume(eq(0L), eq(1L), eq(2L));
        verify(mock, times(1)).consume(eq(3L), eq(4L), eq(5L));
        verify(mock, times(1)).consume(eq(6L), eq(7L), eq(8L));
    }

    interface TriangleCountConsumer {
        void consume(long nodeId, long triangles);
    }

    interface TripleConsumer {
        void consume(long nodeA, long nodeB, long nodeC);
    }
}
