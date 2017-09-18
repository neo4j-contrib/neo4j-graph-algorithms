package org.neo4j.graphalgo.core;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.io.PrintWriter;

final class CypherExporter {
    static void export(PrintWriter out, GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            StringBuilder s = new StringBuilder();
            final ResourceIterable<Node> nodes = db.getAllNodes();
            nodes.forEach(node -> node(node, s).append(System.lineSeparator()));
            s.append("CREATE").append(System.lineSeparator());
            nodes.forEach(node -> node
                    .getRelationships(Direction.OUTGOING)
                    .forEach(rel -> rel(rel, s).append(',').append(System.lineSeparator())));
            s.append(System.lineSeparator());
            out.println(s.toString());
            tx.success();
        }
        out.flush();
    }

    private static StringBuilder node(Node node, StringBuilder s) {
        s.append("CREATE (n").append(node.getId());
        for (Label label : node.getLabels()) {
            s.append(':').append(label.name());
        }
        return s.append(props(node, s)).append(')');
    }

    private static StringBuilder rel(Relationship rel, StringBuilder s) {
        return s
                .append("  (n")
                .append(rel.getStartNode().getId())
                .append(")-[")
                .append(rel.getType().name())
                .append(props(rel, s))
                .append("]->")
                .append("(n")
                .append(rel.getEndNode().getId())
                .append(')');
    }

    private static String props(PropertyContainer prop, StringBuilder s) {
        int length = s.length();
        s.append(" {");
        for (String propKey : prop.getPropertyKeys()) {
            Object propValue = prop.getProperty(propKey);
            s.append(propKey).append(':').append(propValue);
        }
        if (s.length() - 2 != length) {
            s.append('}');
        } else {
            s.setLength(length);
        }
        return "";
    }
}
