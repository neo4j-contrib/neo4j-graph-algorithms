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

import com.carrotsearch.hppc.cursors.IntCursor;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Ignore("rm if verified")
public final class ShortestPathDijkstraUserTest {

    private static final String PATH = "/Users/mknobloch/Downloads";

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {


        String cypher = "" +
                "CREATE CONSTRAINT ON (a:Airport) ASSERT a.IATA IS UNIQUE; " +
                "LOAD CSV FROM 'file://" + PATH + "/airports.txt' as airport " +
                "MERGE (a:Airport {IATA: airport[4]}) " +
                "set  " +
                "a.AirportID = airport[0], " +
                "a.Name = airport[1], " +
                "a.City = airport[2], " +
                "a.Country = airport[3], " +
                "a.ICAO = airport[5], " +
                "a.Latitude = toFloat(airport[6]), " +
                "a.Longitude = toFloat(airport[7]), " +
                "a.Altitude = toInteger(airport[8]), " +
                "a.Timezone = airport[9], " +
                "a.DST=  airport[10], " +
                "a.TZ = airport[11], " +
                "a.Type = airport[12], " +
                "a.Source = airport[13] ;" +
                "create index on :Airport(AirportID); " +
                "LOAD CSV FROM 'file://" + PATH + "/routes.txt' as route " +
                "MATCH (s:Airport {AirportID: route[3]}) " +
                "MATCH (d:Airport {AirportID: route[5]}) " +
                "CREATE (s)-[:Route {Airline:route[1],Codeshare : [6]}]->(d);" +
                "MATCH (a:Airport)-[r:Route]->(b:Airport)\n" +
                "SET r.distance=distance(point({longitude: a.Longitude, latitude : a.Latitude}),point({longitude: b.Longitude, latitude : b.Latitude}))";


        for (String part : cypher.split(";")) {
            System.out.println(part);
            try (Transaction transaction = DB.beginTx()) {
                DB.execute(part);
                transaction.success();
            }
        }

        graph = (HeavyGraph) new GraphLoader(DB)
                .withLabel("Airport")
                .withRelationshipType("Route")
                .withRelationshipWeightsFromProperty("distance", Double.MAX_VALUE)
                .withDirection(Direction.BOTH)
                .load(HeavyGraphFactory.class);
    }

    private static Node getNode(String id) {
        final Pointer.GenericPointer<Node> nodePointer = new Pointer.GenericPointer<>(null);
        DB.execute("MATCH (n:Airport) WHERE n.IATA = '" + id + "' RETURN n").accept(row -> {
            nodePointer.v = row.getNode("n");
            return false;
        });
        return nodePointer.v;
    }

    @Test
    public void test() throws Exception {

        System.out.println("test");

        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(getNode("CFU").getId(),
                        getNode("KRK").getId());

        final Iterator<IntCursor> iterator = dijkstra.getFinalPath().iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next().value);
        }
        System.out.println(dijkstra.getTotalCost());
    }
}
