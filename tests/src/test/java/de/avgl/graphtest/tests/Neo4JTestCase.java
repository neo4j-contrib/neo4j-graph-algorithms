package de.avgl.graphtest.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.api.WeightedGraph;
import org.neo4j.graphalgo.core.GraphLoaders;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * @author mknblch
 *         added 23.02.2017.
 */
public abstract class Neo4JTestCase {

    public static final String WEIGHT_PROPERTY = "weight";
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
        // db.shutdown();
    }

    public static int newVertex() {
        try(Transaction transaction = db.beginTx()) {
            final Node node = db.createNode(Label.label("Node"));
            transaction.success();
            final int id = Math.toIntExact(node.getId());
            return id;
        }
    }

    public static long newEdge(int sourceVertexId, int targetVertexId) {
        try(Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceVertexId);
            final Node target = db.getNodeById(targetVertexId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName("relation"));
            transaction.success();
            return relation.getId() ;
        }
    }

    public static long newEdge(int sourceVertexId, int targetVertexId, double weight) {
        try(Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceVertexId);
            final Node target = db.getNodeById(targetVertexId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName("relation"));
            relation.setProperty(WEIGHT_PROPERTY, weight);
            transaction.success();
            return relation.getId() ;
        }
    }

    public static WeightedGraph lightGraph() {
        return GraphLoaders.lowLevel(db);
    }

//    public static WeightedGraph neo4jView() {
//        return new WeightedGraphView((GraphDatabaseAPI) db, "weight"); // TODO
//    }

    public static WeightedGraph heavyGraph() {
        return GraphLoaders.multiArray(db, WEIGHT_PROPERTY);
    }
}
