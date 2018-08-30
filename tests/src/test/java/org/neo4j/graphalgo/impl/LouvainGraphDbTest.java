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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author mknblch
 */
@Ignore
public class LouvainGraphDbTest {


    public static GraphDatabaseAPI db;
    private static final String PATH = "/Users/mknobloch/neo4j/neo4j-community-3.2.8/data/databases/graph.db/";
    private static Graph graph;

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @BeforeClass
    public static void setup() throws KernelException {
        db = openDb(Paths.get(PATH));
        graph = loadGraph();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LouvainProc.class);
    }

    @Test
    public void test() throws Exception {
        System.out.println(graph.nodeCount());
        final Louvain louvain = new Louvain(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY)
                .withProgressLogger(TestProgressLogger.INSTANCE)
                .compute(5,3);
        System.out.println(louvain.getCommunityCount());
    }

    private static Graph loadGraph() throws KernelException {
        return new GraphLoader(db, Pools.DEFAULT)
                .withLabel("Product")
                .withRelationshipType("CLICKED")
                .withDefaultRelationshipWeight(1.0)
                .asUndirected(true)
//                .withDirection(Direction.OUTGOING)
                .load(HeavyGraphFactory.class);
    }

    private static GraphDatabaseAPI openDb(Path dbLocation) {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbLocation.toFile())
                .setConfig(GraphDatabaseSettings.pagecache_memory, "4G")
                .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
                .newGraphDatabase();
        return (GraphDatabaseAPI) db;
    }

}
