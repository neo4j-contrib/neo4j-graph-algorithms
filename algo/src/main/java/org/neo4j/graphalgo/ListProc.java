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
            " YIELD name, signature, description " +
            " WHERE name starts with 'algo.' AND name <> 'algo.list' AND ($name IS NULL OR name CONTAINS $name) " +
            " RETURN name, signature, description ORDER BY name";

    @Context
    public GraphDatabaseService db;

    @Procedure("algo.list")
    @Description("CALL algo.list - lists all algorithm procedures, their description and signature")
    public Stream<ListResult> list(@Name(value = "name", defaultValue = "null") String name) {
        return db.execute(QUERY, singletonMap("name", name)).stream().map(ListResult::new);
    }

    public static class ListResult {
        public String name;
        public String description;
        public String signature;

        public ListResult(Map<String, Object> row) {
            this.name = (String) row.get("name");
            this.description = (String) row.get("description");
            this.signature = (String) row.get("signature");
        }
    }
}
