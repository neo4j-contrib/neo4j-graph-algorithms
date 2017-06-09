package org.neo4j.graphalgo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * @author mknblch
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
        if (db != null) {
            db.shutdown();
            db = null;
        }
        // calling shutdown takes some 10 seconds or so :(
//         db.shutdown();
    }

    public static int newNode() {
        try (Transaction transaction = db.beginTx()) {
            final Node node = db.createNode(Label.label(LABEL));
            transaction.success();
            final int id = Math.toIntExact(node.getId());
            transaction.success();
            return id;
        }
    }

    public static long newRelation(int sourceNodeId, int targetNodeId) {
        try (Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            transaction.success();
            return relation.getId();
        }
    }

    public static long newRelation(
            long sourceNodeId,
            long targetNodeId,
            double weight) {
        try (Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            relation.setProperty(WEIGHT_PROPERTY, weight);
            transaction.success();
            return relation.getId();
        }
    }
}
