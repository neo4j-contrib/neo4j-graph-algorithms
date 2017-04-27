package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.function.Consumer;

/**
 * RingBuilder creates a ring of nodes where each node is
 * connected to its successor while the last element of the
 * chain connects back to its head.
 *
 * @author mknblch
 */
public class RingBuilder extends GraphBuilder<RingBuilder> {

    protected RingBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    /**
     * create a ring
     *
     * @param size number of elements (>= 2)
     * @return itself for method chaining
     */
    public RingBuilder createRing(int size) {
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
            createRelationship(temp, head);
        });
        return this;
    }

    @Override
    protected RingBuilder me() {
        return this;
    }
}
