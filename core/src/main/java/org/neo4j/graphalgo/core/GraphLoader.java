package org.neo4j.graphalgo.core;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;

public abstract class GraphLoader<G extends Graph> {

    protected abstract void prepare(
            long nodeCount,
            long relsCount,
            int weightProperty);

    protected abstract void readNode(NodeItem node);

    protected abstract G build();

    public final G load(
            GraphDatabaseService graphDatabaseService,
            String relationshipWeightProperty) {
        GraphDatabaseAPI db = (GraphDatabaseAPI) graphDatabaseService;
        try (Transaction tx = db.beginTx();
             Statement statement = db
                     .getDependencyResolver()
                     .resolveDependency(ThreadToStatementContextBridge.class)
                     .get()) {
            ReadOperations read = statement.readOperations();
            final G g = load(read, relationshipWeightProperty);
            tx.success();
            return g;
        }
    }

    public final G load(
            ReadOperations read,
            String relationshipWeightProperty) {
        long nodeCount = read.nodesGetCount();
        long relsCount = read.relationshipsGetCount();
        int weightProp = relationshipWeightProperty != null
                ? read.propertyKeyGetForName(relationshipWeightProperty)
                : StatementConstants.NO_SUCH_PROPERTY_KEY;

        prepare(nodeCount, relsCount, weightProp);

        try (Cursor<NodeItem> allNodes = read.nodeCursorGetAll()) {
            while (allNodes.next()) {
                readNode(allNodes.get());
            }
        }

        return build();
    }
}
