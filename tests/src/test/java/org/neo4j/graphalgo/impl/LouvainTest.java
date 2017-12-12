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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainAlgorithm;
import org.neo4j.graphalgo.impl.louvain.WeightedLouvain;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 *  @author mknblch
 */
@RunWith(Parameterized.class)
public class LouvainTest {

    private static final String unidirectional =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
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
                    " (c)-[:TYPE]->(b),\n" +
                    " (b)-[:TYPE]->(d),\n" +

                    " (e)-[:TYPE]->(f),\n" +
                    " (e)-[:TYPE]->(g),\n" +
                    " (e)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(g),\n" +
                    " (g)-[:TYPE]->(h),\n" +

                    " (e)-[:TYPE {w:5}]->(b)";


    private static final int MAX_ITERATIONS = 10;
    public static final Label LABEL = Label.label("Node");
    public static final String ABCDEFGHZ = "abcdefghz";

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;
    private final Map<String, Integer> nameMap;

    public LouvainTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
        nameMap = new HashMap<>();
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {

        return Arrays.<Object[]>asList(
//                new Object[]{HeavyGraphFactory.class, "heavy"},
//                new Object[]{LightGraphFactory.class, "light"},
//                new Object[]{GraphViewFactory.class, "view"},
                new Object[]{HugeGraphFactory.class, "huge"}
        );
    }
    
    private void setup(String cypher) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("w", 1.0)
                .withDirection(Direction.BOTH)
                .asUndirected(true)
                .load(graphImpl);

        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < ABCDEFGHZ.length(); i++) {
                final String value = String.valueOf(ABCDEFGHZ.charAt(i));
                final int id = graph.toMappedNodeId(DB.findNode(LABEL, "name", value).getId());
                nameMap.put(value, id);
            }
            transaction.success();
        }
    }


    private void assertUnion(String[] nodeNames, int[] communityIds) {
        int current = -1;
        for (String name : nodeNames) {
            if (!nameMap.containsKey(name)) {
                throw new IllegalArgumentException("unknown node name: " + name);
            }
            final int id = nameMap.get(name);
            if (current == -1) {
                current = communityIds[id];
            } else {
                assertEquals("Node " + name + " belongs to wrong community " + communityIds[id], current, communityIds[id]);
            }
        }
    }

    private void assertDisjoint(String[] nodeNames, int[] communityIds) {
        final IntSet set = new IntHashSet();
        for (String name : nodeNames) {
            final int communityId = communityIds[nameMap.get(name)];
            assertTrue("Node " + name + " belongs to wrong community " + communityId, set.add(communityId));
        }
    }

    public void printCommunities(LouvainAlgorithm louvain) {
        try (Transaction transaction = DB.beginTx()) {
            louvain.resultStream()
                    .forEach(r -> {
                        System.out.println(DB.getNodeById(r.nodeId).getProperty("name") + ":" + r.community);
                    });
            transaction.success();
        }
    }

    @Test
    public void testWeightedSequential() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new WeightedLouvain(graph, Pools.DEFAULT, 1, MAX_ITERATIONS)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertWeightedCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    @Test
    public void testUnweightedSequential() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new Louvain(graph, Pools.DEFAULT, 1, MAX_ITERATIONS)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }


    @Ignore("unpredictable due to lack of monotonicity")
    @Test
    public void testUnweightedParallel() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new WeightedLouvain(graph, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY, MAX_ITERATIONS)
                .compute();
        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    public void assertCommunities(LouvainAlgorithm louvain) {
        // TODO should b & e build its own set or belong to either a or f
        assertUnion(new String[]{"a", "c", "d"}, louvain.getCommunityIds());
        assertUnion(new String[]{"f", "g", "h"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "f", "z"}, louvain.getCommunityIds());
    }

    public void assertWeightedCommunities(LouvainAlgorithm louvain) {
        assertCommunities(louvain);
        // TODO should b & e build its own set or belong to either a or f
        assertUnion(new String[]{"b", "e"}, louvain.getCommunityIds());
    }


}
