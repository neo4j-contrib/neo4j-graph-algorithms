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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Disconnected-Graph:
 *
 *  (A)<-->(B)<-->(C)  (D)<-->(E)
 *
 * Calculation:
 *
 * k = N-1 = 4
 *
 *      A     B     C     D     E
 *  --|-----------------------------
 *  A | 0     1     2     -     -    // distance between each pair of nodes
 *  B | 1     0     1     -     -    // or infinite of no path exists
 *  C | 2     1     0     -     -
 *  D | -     -     -     0     1
 *  E | -     -     -     1     0
 *  --|------------------------------
 * -1 |1.5    2    1.5    1     1
 *  ==|==============================
 * *k |0.37  0.12  0.37  0.25  0.25
 *
 * instead of calculating the farness we sum the inverse
 * of each cell and multiply by 1/(n-1)
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class HarmonicCentralityTest {

    private static final double[] EXPECTED = new double[]{0.375, 0.125, 0.375, 0.25, 0.25};

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{LightGraphFactory.class, "Light"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "View"}
        );
    }

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute("CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (b)-[:TYPE]->(a),\n" +
                " (b)-[:TYPE]->(c),\n" +
                " (c)-[:TYPE]->(b),\n" +
                " (d)-[:TYPE]->(e),\n" +
                " (e)-[:TYPE]->(d)");
    }

    private Graph graph;

    public HarmonicCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(graphImpl);
    }


    @Test
    public void testGetCentrality() throws Exception {
        final double[] centrality = new MSHarmonicCentrality(graph, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute()
                .exportToArray();

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }

    @Test
    public void testStream() throws Exception {
        new MSHarmonicCentrality(graph, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute()
                .resultStream()
                .forEach(r -> {
                    assertEquals(EXPECTED[(int) r.nodeId], r.centrality, 0.1);
                });
    }
}
