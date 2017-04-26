package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class RingBuilder extends GraphBuilder<RingBuilder> {


    protected RingBuilder(GraphDatabaseAPI api, String label, String relationship) {
        super(api, label, relationship);
    }


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
