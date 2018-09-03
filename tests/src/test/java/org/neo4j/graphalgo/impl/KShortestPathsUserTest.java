/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.DoubleConsumer;

import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 * Graph:
 *
 *     (b)   (e)
 *   1/ 2\ 1/ 2\
 * >(a)  (d)  (g)
 *   2\ 1/ 2\ 1/
 *    (c)   (f)
 *
 * @author mknblch
 */
public class KShortestPathsUserTest {


    private static final Setting<Boolean> udc = setting("dbms.udc.enabled", BOOLEAN, "true");


    public static final double DELTA = 0.001;

    public static final Path LOCATION = Paths.get("/Users/mknobloch/data/neo4j/airportgraph/graph.db");

    public static GraphDatabaseService db;

    private static Graph graph;

    private static GraphDatabaseAPI openDb(Path dbLocation) {
        return (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbLocation.toFile())
                .setConfig(GraphDatabaseSettings.pagecache_memory, "2G")
                .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
                .setConfig(udc, "false")
                .newGraphDatabase();
    }

    @BeforeClass
    public static void setupGraph() throws KernelException {
        System.out.println("loading db..");
        graph = (HeavyGraph) new GraphLoader(openDb(LOCATION))
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("distance", Double.MAX_VALUE)
                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);

        System.out.println("done");
    }

    @Test
    public void test() throws Exception {

        System.out.println("test");

        final YensKShortestPaths yens = new YensKShortestPaths(graph)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .compute(
                        getNode("KRK").getId(),
                        getNode("DFW").getId(),
                        Direction.BOTH,
                        10, Integer.MAX_VALUE);

        final List<WeightedPath> paths = yens.getPaths();

        final DoubleConsumer mock = mock(DoubleConsumer.class);

        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
            mock.accept(path.getCost());
            System.out.println(path + " = " + path.getCost());
        }
    }

    private static Node getNode(String id) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Airport) WHERE n.IATA = '" + id + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0];
    }
}
