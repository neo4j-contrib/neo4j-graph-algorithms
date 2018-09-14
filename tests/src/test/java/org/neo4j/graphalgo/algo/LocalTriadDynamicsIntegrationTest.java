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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.LocalTriadDynamicsProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedLongLongBitSet;
import org.neo4j.graphalgo.impl.triangle.HugeLocalTriadDynamics;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

/**
 * @author mknblch
 */
public class LocalTriadDynamicsIntegrationTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static HugeGraph graph;

    @BeforeClass
    public static void setup() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" + // center node
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +

                        " (a)-[:TYPE {w:1.0}]->(b),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(c),\n" +
                        " (a)-[:TYPE {w:1.0}]->(d),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(e),\n" +
                        " (a)-[:TYPE {w:1.0}]->(f),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(g),\n" +

                        " (b)-[:TYPE {w:-1.0}]->(c),\n" +
                        " (c)-[:TYPE {w:1.0}]->(d),\n" +
                        " (d)-[:TYPE {w:-1.0}]->(e),\n" +
                        " (e)-[:TYPE {w:1.0}]->(f),\n" +
                        " (f)-[:TYPE {w:-1.0}]->(g),\n" +
                        " (g)-[:TYPE {w:1.0}]->(b)";

        DB.execute(cypher);
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LocalTriadDynamicsProc.class);
    }

    @Test
    public void test() throws Exception {
        DB.execute("CALL algo.localTriadDynamics('Node', 'TYPE', {probability:0.5, weightProperty:'w'})");
        DB.execute("MATCH (n:Node)-[r]->(m:Node) RETURN r, r.w AS w").accept(row -> {
            Relationship r = (Relationship) row.get("r");
            System.out.printf("(%s)-[w:%.1f]->(%s)%n",
                    r.getStartNode().getProperty("name"),
                    row.getNumber("w").doubleValue(),
                    r.getEndNode().getProperty("name"));
            return true;
        });
    }
}
