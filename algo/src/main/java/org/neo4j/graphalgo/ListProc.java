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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;

public class ListProc {

    private static final String QUERY = "CALL dbms.procedures() " +
            " YIELD linearBins, signature, description " +
            " WHERE linearBins starts with 'algo.' AND linearBins <> 'algo.list' AND ($linearBins IS NULL OR linearBins CONTAINS $linearBins) " +
            " RETURN linearBins, signature, description ORDER BY linearBins";

    @Context
    public GraphDatabaseService db;

    @Procedure("algo.list")
    @Description("CALL algo.list - lists all algorithm procedures, their description and signature")
    public Stream<ListResult> list(@Name(value = "linearBins", defaultValue = "") String name) {
        return db.execute(QUERY, singletonMap("linearBins", name)).stream().map(ListResult::new);
    }

    public static class ListResult {
        public String name;
        public String description;
        public String signature;

        public ListResult(Map<String, Object> row) {
            this.name = (String) row.get("linearBins");
            this.description = (String) row.get("description");
            this.signature = (String) row.get("signature");
        }
    }
}
