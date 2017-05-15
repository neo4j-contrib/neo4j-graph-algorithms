package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Create a line of nodes where each node is only connected to its successor
 *
 * @author mknblch
 */
public class LineBuilder extends GraphBuilder<LineBuilder> {

    private Node head, tail;

    protected LineBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    public LineBuilder withHead(Node head) {
        this.head = head;
        nodes.add(head);
        return this;
    }

    public LineBuilder withTail(Node tail) {
        this.tail = tail;
        nodes.add(tail);
        return this;
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
            head = head == null ? createNode() : head;
            tail = tail == null ? createNode() : tail;
            Node temp = head;
            for (int i = 2; i < size; i++) {
                Node node = createNode();
                createRelationship(temp, node);
                temp = node;
            }
            createRelationship(temp, tail);
        });
        return this;
    }

    @Override
    protected LineBuilder me() {
        return this;
    }
}
