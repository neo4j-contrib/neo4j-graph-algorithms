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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 *
 * https://en.wikipedia.org/wiki/Yen%27s_algorithm
 *
 * @author mknblch
 */
public class YensWikiTest {

    private static final int[][] EXPECTED = new int[][]{
        new int[]{0, 2, 3, 5},
        new int[]{0, 2, 4, 5},
        new int[]{0, 2, 3, 4, 5},
    };

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE" +
                        " (c)-[:TYPE {cost:3.0}]->(d),\n" +
                        " (c)-[:TYPE {cost:2.0}]->(e),\n" +
                        " (e)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (e)-[:TYPE {cost:2.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:3.0}]->(g),\n" +
                        " (d)-[:TYPE {cost:4.0}]->(f),\n" +

                        " (f)-[:TYPE {cost:2.0}]->(g),\n" +
                        " (f)-[:TYPE {cost:1.0}]->(h),\n" +
                        " (g)-[:TYPE {cost:2.0}]->(h)";

        db.execute(cypher);

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(Direction.BOTH)
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);

    }
    public static Node getNode(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0];
    }


    @Test
    public void test() throws Exception {

        final YensKShortestPaths yens = new YensKShortestPaths(graph).compute(
                getNode("c").getId(),
                getNode("h").getId(),
                Direction.OUTGOING,
                5);

        final List<WeightedPath> paths = yens.getPaths();
        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath weightedPath = paths.get(i);
            System.out.println(weightedPath + " = " + weightedPath.getCost());
//            assertArrayEquals(EXPECTED[i], weightedPath.toArray());
        }

    }

}
