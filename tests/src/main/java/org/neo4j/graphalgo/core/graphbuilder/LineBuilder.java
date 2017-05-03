package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * RingBuilder creates a ring of nodes where each node is
 * connected to its successor while the last element of the
 * chain connects back to its head.
 *
 * @author mknblch
 */
public class LineBuilder extends GraphBuilder<LineBuilder> {

    protected LineBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    /**
     * create a ring
     *
     * @param size number of elements (>= 2)
     * @return itself for method chaining
     */
    public LineBuilder createLine(int size) {
        if (size < 2) {
            throw new IllegalArgumentException("size must be >= 2");
        }
        withinTransaction(() -> {
            final Node head = createNode();
            Node temp = head;
            for (int i = 1; i < size; i++) {
                Node node = createNode();
                createRelationship(temp, node);
                temp = node;
            }
        });
        return this;
    }

    @Override
    protected LineBuilder me() {
        return this;
    }
}
