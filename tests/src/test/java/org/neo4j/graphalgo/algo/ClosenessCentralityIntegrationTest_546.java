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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.MSClosenessCentrality;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;


/**
 *
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class ClosenessCentralityIntegrationTest_546 {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String importQuery =
                "CREATE (alice:Person{id:\"Alice\"}),\n" +
                "       (michael:Person{id:\"Michael\"}),\n" +
                "       (karin:Person{id:\"Karin\"}),\n" +
                "       (chris:Person{id:\"Chris\"}),\n" +
                "       (will:Person{id:\"Will\"}),\n" +
                "       (mark:Person{id:\"Mark\"})\n" +
                "CREATE (michael)<-[:KNOWS]-(karin),\n" +
                "       (michael)-[:KNOWS]->(chris),\n" +
                "       (will)-[:KNOWS]->(michael),\n" +
                "       (mark)<-[:KNOWS]-(michael),\n" +
                "       (mark)-[:KNOWS]->(will),\n" +
                "       (alice)-[:KNOWS]->(michael),\n" +
                "       (will)-[:KNOWS]->(chris),\n" +
                "       (chris)-[:KNOWS]->(karin);";

        db.execute(importQuery);

    }

    private String name(long id) {
        String[] name = {""};
        db.execute("MATCH (n) WHERE id(n) = " + id + " RETURN n.id as name")
                .accept(row -> {
                    name[0] = row.getString("name");
                    return false;
                });
        if (name[0].isEmpty()) {
            throw new IllegalArgumentException("unknown id " + id);
        }
        return name[0];
    }

    @Test
    public void test() throws Exception {

        final Graph graph = new GraphLoader(db, Pools.DEFAULT)
                .withLabel("Person")
                .withRelationshipType("KNOWS")
                .asUndirected(true)
                .load(HugeGraphFactory.class);

        new MSClosenessCentrality(graph, 2, Pools.DEFAULT)
                .compute()
                .resultStream()
                .forEach(this::print);

    }

    private void print(MSClosenessCentrality.Result result) {
        System.out.println(name(result.nodeId) + ": " + result.centrality);
    }
}
