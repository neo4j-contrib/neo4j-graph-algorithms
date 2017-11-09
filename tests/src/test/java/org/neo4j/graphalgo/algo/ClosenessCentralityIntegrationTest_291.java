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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.ClosenessCentralityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.HugeMSClosenessCentrality;
import org.neo4j.graphalgo.impl.MSBFSCCAlgorithm;
import org.neo4j.graphalgo.impl.MSClosenessCentrality;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.LongToIntFunction;


/**
 * Test for <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/291">Issue 291</a>
 *
 * @author mknblch
 */
@Ignore
@RunWith(Parameterized.class)
public class ClosenessCentralityIntegrationTest_291 {

    private static final String URL = "https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-all-edges.csv";

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{LightGraphFactory.class, "Light"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "Kernel"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.resolveDependency(Procedures.class).registerProcedure(ClosenessCentralityProc.class);

        final String importQuery = String.format("LOAD CSV WITH HEADERS FROM '%s' AS row\n" +
                "MERGE (src:Character {name: row.Source})\n" +
                "MERGE (tgt:Character {name: row.Target})\n" +
                "MERGE (src)-[r:INTERACTS]->(tgt) ON CREATE SET r.weight = toInteger(row.weight)", URL);

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Setup took %d ms%n", l))) {
            DB.execute(importQuery);
        }

    }

    private Class<? extends GraphFactory> graphImpl;
    private String graphName;

    public ClosenessCentralityIntegrationTest_291(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
        this.graphName = name;
    }

    @Test
    public void testDirect() throws Exception {

        final Graph graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Character")
                .withRelationshipType("INTERACTS")
                .withoutNodeWeights()
                .withoutRelationshipWeights()
                .withoutNodeProperties()
                .load(graphImpl);

        MSBFSCCAlgorithm<?> algo;
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            algo = new HugeMSClosenessCentrality(hugeGraph, AllocationTracker.EMPTY, 4, Pools.DEFAULT);
        } else {
            algo = new MSClosenessCentrality(graph, 4, Pools.DEFAULT);
        }


        algo.compute();

        final LongToIntFunction farness = algo.farness();
        final double[] centrality = algo.exportToArray();

        System.out.println("graph.nodeCount() = " + graph.nodeCount());

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.printf("node %3d | farness %3d | closeness %3f%n",
                    i,
                    farness.applyAsInt(i),
                    centrality[i]);
        }
    }

    @Test
    public void test() throws Exception {
        final String callQuery = "CALL algo.closeness('Character', 'INTERACTS', " +
                "{write:true, writeProperty:'closeness', stats:true, concurrency:8, graph:$graph})";
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Algorithm took %d ms%n", l))) {
            DB.execute(callQuery, MapUtil.map("graph", graphName));
        }

        final String testQuery = "MATCH (c:Character) WHERE exists(c.closeness) RETURN c.name as name, " +
                "c.closeness as closeness ORDER BY closeness DESC LIMIT 50";
        DB.execute(testQuery).accept(row -> {
            final String name = row.getString("name");
            final double closeness = row.getNumber("closeness").doubleValue();
            System.out.printf("%s = %3f%n", name, closeness);
            return true;
        });
    }
}
