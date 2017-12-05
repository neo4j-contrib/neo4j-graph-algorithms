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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.LouvainAlgorithm;
import org.neo4j.graphalgo.impl.louvain.ParallelWeightedLouvain;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 *  @author mknblch
 */
@RunWith(Parameterized.class)
public class WeightedLouvainModularityTest {

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
                    " (b)-[:TYPE]->(c),\n" +
                    " (b)-[:TYPE]->(d),\n" +

                    " (e)-[:TYPE]->(f),\n" +
                    " (e)-[:TYPE]->(g),\n" +
                    " (e)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(h),\n" +
                    " (f)-[:TYPE]->(g),\n" +
                    " (g)-[:TYPE]->(h),\n" +

                    " (e)-[:TYPE {w:10}]->(b)";


    private static final String bidirectional =
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
                    " (b)-[:TYPE]->(a),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(a),\n" +
                    " (a)-[:TYPE]->(d),\n" +
                    " (d)-[:TYPE]->(a),\n" +
                    " (c)-[:TYPE]->(d),\n" +
                    " (d)-[:TYPE]->(c),\n" +
                    " (b)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(b),\n" +
                    " (b)-[:TYPE]->(d),\n" +
                    " (d)-[:TYPE]->(b),\n" +

                    " (e)-[:TYPE]->(f),\n" +
                    " (f)-[:TYPE]->(e),\n" +
                    " (e)-[:TYPE]->(g),\n" +
                    " (g)-[:TYPE]->(e),\n" +
                    " (e)-[:TYPE]->(h),\n" +
                    " (h)-[:TYPE]->(e),\n" +
                    " (f)-[:TYPE]->(h),\n" +
                    " (h)-[:TYPE]->(f),\n" +
                    " (f)-[:TYPE]->(g),\n" +
                    " (g)-[:TYPE]->(f),\n" +
                    " (g)-[:TYPE]->(h),\n" +
                    " (h)-[:TYPE]->(g),\n" +

                    " (b)-[:TYPE {w:5}]->(e),\n" +
                    " (e)-[:TYPE {w:5}]->(b)";

    private static final int MAX_ITERATIONS = 5;

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;
    private RelationshipWeights weights;
    private final Map<String, Integer> nodeIdMap;

    public WeightedLouvainModularityTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
        nodeIdMap = new HashMap<>();
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {

        return Arrays.<Object[]>asList(
                new Object[]{HeavyGraphFactory.class, "heavy"},
                new Object[]{LightGraphFactory.class, "light"},
                new Object[]{GraphViewFactory.class, "view"}

                // new Object[]{HugeGraphFactory.class, "huge"} // TODO
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
                .load(graphImpl);
        weights = (RelationshipWeights) graph;
        final Label label = Label.label("Node");
        String nodeIds = "abcdefghz";
        try (Transaction transaction = DB.beginTx()) {
            for (int i = 0; i < nodeIds.length(); i++) {
                final String value = String.valueOf(nodeIds.charAt(i));
                final int id = graph.toMappedNodeId(DB.findNode(label, "name", value).getId());
                nodeIdMap.put(value, id);
            }
            transaction.success();
        }
    }


    private String getName(long nodeId) {
        String[] name = {""};
        DB.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
            name[0] = (String) row.getNode("n").getProperty("name");
            return true;
        });
        return name[0];
    }

    private void assertUnion(String[] nodeNames, int[] communityIds) {
        int current = -1;
        for (String name : nodeNames) {
            if (!nodeIdMap.containsKey(name)) {
                throw new IllegalArgumentException("unknown node name: " + name);
            }
            final int id = nodeIdMap.get(name);
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
            final int communityId = communityIds[nodeIdMap.get(name)];
            assertTrue("Node " + name + " belongs to wrong community " + communityId, set.add(communityId));
        }
    }

    public void printCommunities(LouvainAlgorithm louvain) {
        louvain.resultStream()
                .forEach(r -> {
                    System.out.println(getName(r.nodeId) + ":" + r.community);
                });
    }

    @Test
    public void testUnidirectional() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new ParallelWeightedLouvain(graph, graph, weights, Pools.DEFAULT, 1, MAX_ITERATIONS)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertUnidirectionalCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    @Test
    public void testBidirectional() throws Exception {
        setup(bidirectional);
        final LouvainAlgorithm louvain = new ParallelWeightedLouvain(graph, graph, weights, Pools.DEFAULT, 1, 10)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertBidirectionalCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    @Test
    public void testBidirectionalParallel() throws Exception {
        setup(bidirectional);
        final LouvainAlgorithm louvain = new ParallelWeightedLouvain(graph, graph, weights, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY, 10)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertBidirectionalCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    @Test
    public void testUnidirectionalParallel() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new ParallelWeightedLouvain(graph, graph, weights, Pools.DEFAULT, Pools.DEFAULT_CONCURRENCY, 10)
                .compute();

        printCommunities(louvain);
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertUnidirectionalCommunities(louvain);
        assertTrue("Maximum iterations > " + MAX_ITERATIONS,louvain.getIterations() < MAX_ITERATIONS);
    }

    public void assertBidirectionalCommunities(LouvainAlgorithm louvain) {
        assertUnion(new String[]{"b", "e"}, louvain.getCommunityIds());
        assertUnion(new String[]{"a", "c", "d"}, louvain.getCommunityIds());
        assertUnion(new String[]{"f", "g", "h"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "b", "f", "z"}, louvain.getCommunityIds());
    }


    public void assertUnidirectionalCommunities(LouvainAlgorithm louvain) {
        // TODO where should b and e belong to
        assertUnion(new String[]{"a", "c", "d"}, louvain.getCommunityIds());
        assertUnion(new String[]{"f", "g", "h"}, louvain.getCommunityIds());
        assertDisjoint(new String[]{"a", "f", "z"}, louvain.getCommunityIds());
    }


}
