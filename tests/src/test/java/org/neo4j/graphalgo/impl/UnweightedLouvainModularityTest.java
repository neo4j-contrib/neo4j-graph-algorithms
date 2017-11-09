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

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainAlgorithm;
import org.neo4j.graphalgo.impl.louvain.ParallelLouvain;
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
public class UnweightedLouvainModularityTest {

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

                    " (e)-[:TYPE]->(b)";


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

                    " (b)-[:TYPE]->(e),\n" +
                    " (e)-[:TYPE]->(b)";


    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private final String cypher;
    private Graph graph;

    public UnweightedLouvainModularityTest(
            Class<? extends GraphFactory> graphImpl,
            String name, String cypher) {
        this.graphImpl = graphImpl;
        this.cypher = cypher;
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {

        return Arrays.<Object[]>asList(
                new Object[]{HeavyGraphFactory.class, "heavy", unidirectional},
                new Object[]{HeavyGraphFactory.class, "heavy", bidirectional},
                new Object[]{LightGraphFactory.class, "light", unidirectional},
                new Object[]{LightGraphFactory.class, "light", bidirectional},
                new Object[]{HugeGraphFactory.class, "huge", unidirectional},
                new Object[]{HugeGraphFactory.class, "huge", bidirectional}
//                new Object[]{GraphViewFactory.class, "view"}
        );
    }
    
    private void setup(String cypher, Direction direction) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(direction)
                .load(graphImpl);
    }

    @Test
    public void testUnweighted() throws Exception {
        setup(cypher, Direction.BOTH);
        final LouvainAlgorithm louvain = new Louvain(graph, graph, graph, Pools.DEFAULT, 1, 10)
                .compute();
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertEquals(3, louvain.getCommunityCount());
    }

    @Test
    public void testParallel() throws Exception {
        setup(cypher, Direction.BOTH);
        final LouvainAlgorithm louvain = new ParallelLouvain(graph, graph, graph, Pools.DEFAULT, 1, 10)
                .compute();
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        System.out.println("louvain.getCommunityCount() = " + louvain.getCommunityCount());
        assertEquals(3, louvain.getCommunityCount());
    }

}
