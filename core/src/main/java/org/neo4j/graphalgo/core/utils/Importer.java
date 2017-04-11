package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * The class intends to ease the import of data sources
 *
 * @author mknblch
 */
public abstract class Importer<T, ME extends Importer<T, ME>> {

    protected final GraphDatabaseAPI api;
    protected final ThreadToStatementContextBridge bridge;
    protected GraphSetup setup;

    protected int labelId = StatementConstants.NO_SUCH_LABEL;
    protected int[] relationId = null;
    protected int propertyId = StatementConstants.NO_SUCH_PROPERTY_KEY;

    protected int nodeCount = 0;

    public Importer(GraphDatabaseAPI api) {
        this.api = api;
        this.bridge = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    /**
     * instructs the importer to use constraints set in the GraphSetup
     *
     * @param setup the setup object
     * @return this for method chaining
     */
    public ME withGraphSetup(GraphSetup setup) {
        this.setup = setup;

        withinTransaction(readOp -> {
            labelId = setup.loadAnyLabel()
                    ? ReadOperations.ANY_LABEL
                    : readOp.labelGetForName(setup.startLabel);
            if (!setup.loadAnyRelationshipType()) {
                int relId = readOp.relationshipTypeGetForName(setup.relationshipType);
                if (relId != StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                    relationId = new int[]{relId};
                }
            }
            nodeCount = Math.toIntExact(readOp.countsForNode(labelId));
            propertyId = setup.loadAnyProperty()
                    ? StatementConstants.NO_SUCH_PROPERTY_KEY
                    : readOp.propertyKeyGetForName(setup.propertyName);
        });

        return me();
    }

    /**
     * executes a consumer within its own transaction
     *
     * @param block the consumer
     */
    protected final void withinTransaction(Consumer<ReadOperations> block) {
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
            block.accept(statement.readOperations());
            tx.success();
        }
    }

    /**
     * getThis-trick for method chaining in child classes
     * @return return self
     */
    protected abstract ME me();

    public abstract T build();
}
