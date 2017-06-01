package org.neo4j.graphalgo.core.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a grid of nodes
 *
 * A -- B -- C -- D -- E -- F ..
 * |    |    |    |    |    |
 * G -- H -- I -- J -- K -- L ..
 * |    |    |    |    |    |
 * ..   ..   ..   ..   ..   ..
 *
 * @author mknblch
 */
public class GridBuilder extends GraphBuilder<GridBuilder> {

    private List<List<Node>> lines = new ArrayList<>();

    protected GridBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship) {
        super(api, label, relationship);
    }

    public GridBuilder createGrid(int width, int height) {
        withinTransaction(() -> {
            List<Node> temp = null;
            for (int i = 0; i < height; i++) {
                List<Node> line = createLine(width);
                if (null != temp) {
                    for (int j = 0; j < width; j++) {
                        createRelationship(temp.get(j), line.get(j));
                    }
                }
                temp = line;
            }
        });
        return this;
    }

    private List<Node> createLine(int length) {
        ArrayList<Node> nodes = new ArrayList<>();
        Node temp = createNode();
        nodes.add(temp);
        for (int i = 1; i < length; i++) {
            Node node = createNode();
            nodes.add(temp);
            createRelationship(temp, node);
            temp = node;
        }
        lines.add(nodes);
        return nodes;
    }

    public List<List<Node>> getLineNodes() {
        return lines;
    }

    @Override
    protected GridBuilder me() {
        return this;
    }
}
