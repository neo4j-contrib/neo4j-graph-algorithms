package org.neo4j.graphalgo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * @author mknblch
 *         added 23.02.2017.
 */
public abstract class Neo4JTestCase {

    public static final String LABEL = "Node";
    public static final String WEIGHT_PROPERTY = "weight";
    public static final String RELATION = "RELATION";

    protected static GraphDatabaseService db;

    @BeforeClass
    public static void setup() {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
    }

    @AfterClass
    public static void teardown() {
        // calling shutdown takes some 10 seconds or so :(
//         db.shutdown();
    }

    public static int newNode() {
        try(Transaction transaction = db.beginTx()) {
            final Node node = db.createNode(Label.label(LABEL));
            transaction.success();
            final int id = Math.toIntExact(node.getId());
            return id;
        }
    }

    public static long newRelation(int sourceNodeId, int targetNodeId) {
        try(Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            transaction.success();
            return relation.getId() ;
        }
    }

    public static long newRelation(long sourceNodeId, long targetNodeId, double weight) {
        try(Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            relation.setProperty(WEIGHT_PROPERTY, weight);
            transaction.success();
            return relation.getId() ;
        }
    }
}
