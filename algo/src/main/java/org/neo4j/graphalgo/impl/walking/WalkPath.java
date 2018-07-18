package org.neo4j.graphalgo.impl.walking;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class WalkPath implements Path {
    public static final Path EMPTY = new WalkPath(0);

    private List<Node> nodes;
    private List<Relationship> relationships;
    private final int size;

    public WalkPath(int size) {
        nodes = new ArrayList<>(size);
        relationships = new ArrayList<>(Math.max(0, size - 1)); // for empty paths
        this.size = size;
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void addRelationship(Relationship relationship) {
        relationships.add(relationship);
    }

    @Override
    public Node startNode() {
        return size==0 ? null : nodes.get(0);
    }

    @Override
    public Node endNode() {
        return size==0 ? null : nodes.get(nodes.size() - 1);
    }

    @Override
    public Relationship lastRelationship() {
        return size==0 ? null : relationships.get(relationships.size() - 1);
    }

    @Override
    public Iterable<Relationship> relationships() {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        ArrayList<Relationship> reverse = new ArrayList<>(relationships);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public Iterable<Node> nodes() {
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes() {
        ArrayList<Node> reverse = new ArrayList<>(nodes);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public int length() {
        return size-1;
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    @Override
    public Iterator<PropertyContainer> iterator() {
        return new Iterator<PropertyContainer>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i < 2 * size;
            }

            @Override
            public PropertyContainer next() {
                PropertyContainer pc = i % 2 == 0 ? nodes.get(i / 2) : relationships.get(i / 2);
                i++;
                return pc;
            }
        };
    }
}
