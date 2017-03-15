package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknobloch
 */
public abstract class AbstractGraphLoader {

    private String label = null;
    private String relation = null;
    private String weightProperty = null;

    protected int labelId;
    protected int relationId;
    protected int nodeCount;
    protected int relationCount;
    protected int weightId;

    public AbstractGraphLoader setLabel(String label) {
        this.label = label;
        return this;
    }

    public AbstractGraphLoader setRelation(String relation) {
        this.relation = relation;
        return this;
    }

    public AbstractGraphLoader setWeightProperty(String weightProperty) {
        this.weightProperty = weightProperty;
        return this;
    }

    public Graph load(GraphDatabaseAPI api) {
        try(Transaction tx = api.beginTx();
            Statement statement = api
                    .getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()) {
            final ReadOperations read = statement.readOperations();
            labelId = read.labelGetForName(label);
            relationId = read.relationshipTypeGetForName(relation);
            nodeCount = Math.toIntExact(read.countsForNode(labelId));
            relationCount = Math.toIntExact(read.relationshipsGetCount());
            weightId = read.propertyKeyGetForName(weightProperty);
            final Graph graph = load(read, labelId, relationId, weightId, nodeCount, relationCount);
            tx.success();
            return graph;
        }
    }

    protected abstract Graph load(ReadOperations read, int labelId, int relationId, int weightId, int nodeCount, int relationCount);

}
