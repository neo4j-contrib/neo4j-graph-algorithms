package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.AbstractGraphLoader;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

/**
 * @author mknobloch
 */
public class HeavyGraphFactory extends GraphFactory {

    protected int labelId;
    protected int relationId;
    protected int nodeCount;
    protected int relationCount;
    protected int weightId;

    public HeavyGraphFactory(ReadOperations readOp, String label, String relation, String property) {
        super(readOp, label, relation, property);
        labelId = readOp.labelGetForName(label);
        relationId = readOp.relationshipTypeGetForName(relation);
        nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
        relationCount = Math.toIntExact(readOp.relationshipsGetCount());
        weightId = readOp.propertyKeyGetForName(property);
    }

    @Override
    public Graph build() {

        final IdMap idMap = new IdMap(nodeCount);
        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);
        final WeightMapping weightMapping = relationId == NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(0)
                : new WeightMapping(nodeCount);

        if (labelId == StatementConstants.NO_SUCH_LABEL) {
            try(Cursor<NodeItem> cursor = readOp.nodeCursorGetAll()) {
                while (cursor.next()) {
                    readNode(relationId, idMap, matrix, weightMapping, cursor);
                }
            }
        } else {
            try(Cursor<NodeItem> cursor = readOp.nodeCursorGetForLabel(labelId)) {
                while (cursor.next()) {
                    readNode(relationId, idMap, matrix, weightMapping, cursor);
                }
            }
        }

        return new HeavyGraph(idMap, matrix, weightMapping);
    }

    private void readNode(int relationId, IdMap idMap, AdjacencyMatrix matrix, WeightMapping weightMapping, Cursor<NodeItem> nodeItemCursor) {
        final NodeItem node = nodeItemCursor.get();
        final int outDegree = node.degree(Direction.OUTGOING);
        final int inDegree = node.degree(Direction.INCOMING);
        final int nodeId = idMap.mapOrGet(node.id());
        matrix.armOut(nodeId, outDegree);
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.OUTGOING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long targetNodeId = rel.otherNode(node.id());
                final int targetGraphId = idMap.mapOrGet(targetNodeId);
                final long edgeId = rel.id();
                try (Cursor<PropertyItem> weights = rel.property(relationId)) {
                    while (weights.next()) {
                        weightMapping.add(edgeId, weights.get().value());
                    }
                }
                matrix.addOutgoing(nodeId, targetGraphId, edgeId);
            }
        }
        matrix.armIn(nodeId, inDegree);
        try (Cursor<RelationshipItem> rels = node.relationships(Direction.INCOMING)) {
            while (rels.next()) {
                final RelationshipItem rel = rels.get();
                final long targetNodeId = rel.otherNode(node.id());
                final int targetGraphId = idMap.mapOrGet(targetNodeId);
                final long edgeId = rel.id();
                matrix.addIncoming(targetGraphId, nodeId, edgeId);
            }
        }
    }
}
