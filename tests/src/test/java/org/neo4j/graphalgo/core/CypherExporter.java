package org.neo4j.graphalgo.core;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;

import java.io.PrintWriter;
import java.util.stream.Collectors;

final class CypherExporter {
    static void export(PrintWriter out, GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            final ResourceIterable<Node> nodes = db.getAllNodes();
            nodes
                    .stream()
                    .forEach(node -> out.println("CREATE (n" + node.getId() + ")"));
            out.println(nodes
                    .stream()
                    .flatMap(
                            node -> Iterables.stream(
                                    node.getRelationships(Direction.OUTGOING)
                            )
                    )
                    .map(rel ->
                            "  (n" + rel.getStartNode().getId() + ")"
                                    + "-[:TYPE]->"
                                    + "(n" + rel.getEndNode().getId() + ")"
                    )
                    .collect(Collectors.joining(",\n", "CREATE\n", "\n"))
            );
            tx.success();
        }
        out.flush();
    }
}
