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
import org.neo4j.graphalgo.ListProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 20.10.17
 */
public class ListProcTest {
    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setUp() throws Exception {
        Procedures procedures = DB.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(ListProc.class);
        procedures.registerProcedure(PageRankProc.class);
    }

    @Test
    public void list() throws Exception {
        assertEquals(asList("algo.pageRank","algo.pageRank.stream"), listProcs(null));
        assertEquals(asList("algo.pageRank","algo.pageRank.stream"), listProcs("page"));
        assertEquals(singletonList("algo.pageRank.stream"), listProcs("stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    private List<String> listProcs(Object name) {
        return DB.execute("CALL algo.list($name)", singletonMap("name", name)).<String>columnAs("name").stream().collect(Collectors.toList());
    }
    @Test
    public void listEmpty() throws Exception {
        assertEquals(asList("algo.pageRank","algo.pageRank.stream"),
                DB.execute("CALL algo.list()").<String>columnAs("name").stream().collect(Collectors.toList()));
    }
}
