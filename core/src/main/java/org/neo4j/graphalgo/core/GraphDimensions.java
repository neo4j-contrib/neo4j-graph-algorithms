package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.StatementTask;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphDimensions extends StatementTask<GraphDimensions, RuntimeException> {
    private final GraphSetup setup;

    private long nodeCount;
    private long allNodesCount;
    private long maxRelCount;
    private int labelId;
    private int[] relationId;
    private int weightId;
    private int relWeightId;
    private int nodeWeightId;
    private int nodePropId;

    public GraphDimensions(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api);
        this.setup = setup;
    }

    public long hugeNodeCount() {
        return nodeCount;
    }

    public long allNodesCount() {
        return allNodesCount;
    }

    public int nodeCount() {
        return Math.toIntExact(nodeCount);
    }

    public long maxRelCount() {
        return maxRelCount;
    }

    public int labelId() {
        return labelId;
    }

    public int[] relationId() {
        return relationId;
    }

    public int weightId() {
        return weightId;
    }

    public int relWeightId() {
        return relWeightId;
    }

    public int nodeWeightId() {
        return nodeWeightId;
    }

    public int nodePropId() {
        return nodePropId;
    }

    @Override
    public GraphDimensions apply(final Statement statement) throws RuntimeException {
        final ReadOperations readOp = statement.readOperations();
        labelId = setup.loadAnyLabel()
                ? ReadOperations.ANY_LABEL
                : readOp.labelGetForName(setup.startLabel);
        if (!setup.loadAnyRelationshipType()) {
            int relId = readOp.relationshipTypeGetForName(setup.relationshipType);
            if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                relationId = new int[]{relId};
            }
        }
        weightId = setup.loadDefaultRelationshipWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
        relWeightId = setup.loadDefaultRelationshipWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.relationWeightPropertyName);
        nodeWeightId = setup.loadDefaultNodeWeight()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.nodeWeightPropertyName);
        nodePropId = setup.loadDefaultNodeProperty()
                ? StatementConstants.NO_SUCH_PROPERTY_KEY
                : readOp.propertyKeyGetForName(setup.nodePropertyName);
        nodeCount = readOp.countsForNode(labelId);
        allNodesCount = readOp.nodesGetCount();
        maxRelCount = Math.max(
                readOp.countsForRelationshipWithoutTxState(labelId, relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0], ReadOperations.ANY_LABEL),
                readOp.countsForRelationshipWithoutTxState(ReadOperations.ANY_LABEL, relationId == null ? ReadOperations.ANY_RELATIONSHIP_TYPE : relationId[0], labelId)
        );
        return this;
    }
}
