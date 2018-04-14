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

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ShortestPathIntegrationTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public final ImpermanentDatabaseRule db_599 = new ImpermanentDatabaseRule().startLazily();

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Node{type:'start'})\n" + // start
                        "CREATE (nB:Node)\n" +
                        "CREATE (nC:Node)\n" +
                        "CREATE (nD:Node)\n" +
                        "CREATE (nX:Node{type:'end'})\n" + // end
                        "CREATE\n" +

                        // sum: 5.0
                        "  (nA)-[:TYPE {cost:5.0}]->(nX),\n" +
                        // sum: 4.0
                        "  (nA)-[:TYPE {cost:2.0}]->(nB),\n" +
                        "  (nB)-[:TYPE {cost:2.0}]->(nX),\n" +
                        // sum: 3.0
                        "  (nA)-[:TYPE {cost:1.0}]->(nC),\n" +
                        "  (nC)-[:TYPE {cost:1.0}]->(nD),\n" +
                        "  (nD)-[:TYPE {cost:1.0}]->(nX)";

        DB.execute(createGraph).close();
        DB.resolveDependency(Procedures.class).registerProcedure(ShortestPathProc.class);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Huge"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testDijkstraStream() throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath.stream(start, end, 'cost',{graph:'" + graphImpl + "'}) " +
                        "YIELD nodeId, cost RETURN nodeId, cost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept((Long) row.getNumber("nodeId"), (Double) row.getNumber("cost"));
                    return true;
                });
        verify(consumer, times(4)).accept(anyLong(), anyDouble());
        verify(consumer, times(1)).accept(anyLong(), eq(0.0));
        verify(consumer, times(1)).accept(anyLong(), eq(1.0));
        verify(consumer, times(1)).accept(anyLong(), eq(2.0));
        verify(consumer, times(1)).accept(anyLong(), eq(3.0));
    }

    @Test
    public void testDijkstra() throws Exception {
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath(start, end, 'cost',{graph:'" + graphImpl + "', write:true, writeProperty:'step'}) " +
                        "YIELD loadMillis, evalMillis, writeMillis, nodeCount, totalCost\n" +
                        "RETURN loadMillis, evalMillis, writeMillis, nodeCount, totalCost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3.0, (Double) row.getNumber("totalCost"), 10E2);
                    assertEquals(4L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadMillis"));
                    assertNotEquals(-1L, row.getNumber("evalMillis"));
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    return false;
                });

        final StepConsumer mock = mock(StepConsumer.class);

        DB.execute("MATCH (n) WHERE exists(n.step) RETURN id(n) as id, n.step as step")
                .accept(row -> {
                    mock.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("step").intValue());
                    return true;
                });

        verify(mock, times(4)).accept(anyLong(), anyInt());

        verify(mock, times(1)).accept(anyLong(), eq(0));
        verify(mock, times(1)).accept(anyLong(), eq(1));
        verify(mock, times(1)).accept(anyLong(), eq(2));
        verify(mock, times(1)).accept(anyLong(), eq(3));
    }

    /** @see <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/599">Issue #599</a> */
    @Test
    public void test599() throws KernelException {
        db_599.resolveDependency(Procedures.class).registerProcedure(ShortestPathProc.class);
        final String create = "CREATE\n" +
                "    (v1:Node {VID: 1})\n" +
                "  , (v2:Node {VID: 2})\n" +
                "  , (v3:Node {VID: 3})\n" +
                "  , (v4:Node {VID: 4})\n" +
                "  , (v5:Node {VID: 5})\n" +
                "  , (v6:Node {VID: 6})\n" +
                "  , (v7:Node {VID: 7})\n" +
                "  ,(v1)-[:EDGE {WEIGHT: 0.5}]->(v2)\n" +
                "  ,(v1)-[:EDGE {WEIGHT: 5.0}]->(v3)\n" +
                "  ,(v2)-[:EDGE {WEIGHT: 0.5}]->(v5)\n" +
                "  ,(v3)-[:EDGE {WEIGHT: 2.0}]->(v4)\n" +
                "  ,(v5)-[:EDGE {WEIGHT: 0.5}]->(v6)\n" +
                "  ,(v6)-[:EDGE {WEIGHT: 0.5}]->(v3)\n" +
                "  ,(v6)-[:EDGE {WEIGHT: 23.0}]->(v7)\n" +
                "  ,(v1)-[:EDGE {WEIGHT: 5.0}]->(v4)\n" +
                "";
        db_599.execute(create);

        final String totalCostCommand = "" +
                "MATCH (startNode {VID: 1}), (endNode {VID: 4})\n" +
                "CALL algo.shortestPath(startNode, endNode, 'WEIGHT', {direction: 'OUTGOING'})\n" +
                "YIELD nodeCount, totalCost, loadMillis, evalMillis, writeMillis\n" +
                "RETURN totalCost\n";

        double totalCost = db_599
                .execute(totalCostCommand)
                .<Double>columnAs("totalCost")
                .stream()
                .findFirst()
                .orElse(Double.NaN);

        assertEquals(4.0, totalCost, 1e-4);

        final String pathCommand = "" +
                "MATCH (startNode {VID: 1}), (endNode {VID: 4})\n" +
                "CALL algo.shortestPath.stream(startNode, endNode, 'WEIGHT', {direction: 'OUTGOING'})\n" +
                "YIELD nodeId, cost\n" +
                "MATCH (n1) WHERE id(n1) = nodeId\n" +
                "RETURN n1.VID as id, cost as weight\n";

        List<Matcher<Number>> expectedList = Arrays.asList(
                is(1L), is(0.0),
                is(2L), is(0.5),
                is(5L), is(1.0),
                is(6L), is(1.5),
                is(3L), is(2.0),
                is(4L), is(4.0));
        Iterator<Matcher<Number>> expected = expectedList.iterator();

        final Result pathResult = db_599.execute(pathCommand);
        pathResult.forEachRemaining(res -> {
            assertThat((Number) res.get("id"), expected.next());
            assertThat((Number) res.get("weight"), expected.next());
        });
        pathResult.close();
    }

    private interface PathConsumer {
        void accept(long nodeId, double cost);
    }

    interface StepConsumer {
        void accept(long nodeId, int step);
    }
}
