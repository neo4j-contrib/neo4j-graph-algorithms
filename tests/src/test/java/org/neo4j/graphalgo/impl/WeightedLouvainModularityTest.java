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

import org.junit.Ignore;
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
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.LouvainAlgorithm;
import org.neo4j.graphalgo.impl.louvain.WeightedLouvain;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

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


    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;
    private RelationshipWeights weights;

    public WeightedLouvainModularityTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
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
    }


    private String getName(long nodeId) {
        String[] name = {""};
        DB.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
            name[0] = (String) row.getNode("n").getProperty("name");
            return true;
        });
        return name[0];
    }

    @Test
    public void testUnidirectional() throws Exception {
        setup(unidirectional);
        final LouvainAlgorithm louvain = new WeightedLouvain(graph, graph, weights, Pools.DEFAULT, 1, 10)
                .compute();

        louvain.resultStream()
                .forEach(r -> {
                    System.out.println(getName(r.nodeId) + ":" + r.community);
                });

        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertEquals(4, louvain.getCommunityCount());
    }

    @Ignore
    @Test
    public void testBidirectional() throws Exception {
        setup(bidirectional);
        final LouvainAlgorithm louvain = new WeightedLouvain(graph, graph, weights, Pools.DEFAULT, 1, 10)
                .compute();

        louvain.resultStream()
                .forEach(r -> {
                    System.out.println(getName(r.nodeId) + ":" + r.community);
                });

        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertEquals(4, louvain.getCommunityCount());
    }

}
