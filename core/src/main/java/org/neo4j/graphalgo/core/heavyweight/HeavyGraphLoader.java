package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

/**
 * @author mknobloch
 */
public class HeavyGraphLoader {

    protected String label = null;

    protected String relationType = null;

    public HeavyGraphLoader setLabel(String label) {
        this.label = label;
        return this;
    }

    public HeavyGraphLoader setRelationType(String relationType) {
        this.relationType = relationType;
        return this;
    }

    public WeightedGraph load(GraphDatabaseAPI api) {
        try(Transaction tx = api.beginTx();
            Statement statement = api
                    .getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()) {
            final ReadOperations readOperations = statement.readOperations();
            final WeightedGraph graph = load(readOperations);
            tx.success();
            return graph;
        }
    }

    private WeightedGraph load(ReadOperations read) {

        final int labelId = read.labelGetForName(label);
        final int relationId = read.relationshipTypeGetForName(relationType);
        final int nodeCount = Math.toIntExact(read.countsForNode(labelId));

        final Cursor<NodeItem> nodeItemCursor;
        if (NO_SUCH_LABEL == labelId) {
            nodeItemCursor = read.nodeCursorGetAll(); // TODO thr exception?
        } else {
            nodeItemCursor = read.nodeCursorGetForLabel(labelId);
        }

        final IdMap idMap = new IdMap(nodeCount);
        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);
        final WeightMapping weightMapping = relationId == NO_SUCH_PROPERTY_KEY
                ? new WeightMapping(0)
                : new WeightMapping(nodeCount);

        while (nodeItemCursor.next()) {
            final NodeItem node = nodeItemCursor.get();
            final int nodeId = idMap.mapOrGet(node.id());
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
                    matrix.add(nodeId, targetGraphId, edgeId);
                }
            }
        }
        return new HeavyGraph(idMap, matrix, weightMapping);
    }
}
