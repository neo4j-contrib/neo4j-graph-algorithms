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
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.function.DoubleConsumer;

import static org.mockito.Mockito.mock;

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

    public static final double DELTA = 0.001;

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String load =
                "LOAD CSV FROM 'file:///Users/mknobloch/Downloads/airports.txt' as airport\n" +
                        "CREATE (:Airport {\n" +
                        "AirportID : airport[0],\n" +
                        "Name : airport[1],\n" +
                        "City : airport[2],\n" +
                        "Country : airport[3],\n" +
                        "IATA: airport[4],\n" +
                        "ICAO: airport[5],\n" +
                        "Latitude: toFloat(airport[6]),\n" +
                        "Longitude: toFloat(airport[7]),\n" +
                        "Altitude: toInteger(airport[8]),\n" +
                        "Timezone: airport[9],\n" +
                        "DST: airport[10],\n" +
                        "TZ: airport[11],\n" +
                        "Type: airport[12],\n" +
                        "Source: airport[13]\n" +
                        "});";

        final String constraint =
                        "CREATE CONSTRAINT ON (a:Airport) ASSERT a.IATA IS UNIQUE;";
        final String routes =
                        "LOAD CSV FROM 'file:///Users/mknobloch/Downloads/routes.txt' as route\n" +
                        "MATCH (s:Airport {AirportID: route[3]})\n" +
                        "MATCH (d:Airport {AirportID: route[5]})\n" +
                        "CREATE (s)-[:Route {Airline:route[1],Codeshare : [6]}]->(d);";
        final String dist =
                        "MATCH (a:Airport)-[r:Route]->(b:Airport)\n" +
                        "SET r.distance=distance(point({longitude: a.Longitude, latitude : a.Latitude}),point({longitude: b.Longitude, latitude : b.Latitude}));";

        System.out.println("execute: " + load);
        db.execute(load);
//        db.execute(constraint);
        System.out.println("execute: " + routes);
        db.execute(routes);
        System.out.println("execute: " + dist);
        db.execute(dist);

        System.out.println("loading db..");

        graph = (HeavyGraph) new GraphLoader(db)
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
                        4, Integer.MAX_VALUE);

        final List<WeightedPath> paths = yens.getPaths();

        final DoubleConsumer mock = mock(DoubleConsumer.class);

        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath path = paths.get(i);
            mock.accept(path.getCost());
            System.out.println(path + " = "  + path.getCost());
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
