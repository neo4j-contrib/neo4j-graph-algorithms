package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author mknblch
 */
public class ParallelUnionFindTest {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    @BeforeClass
    public static void setup() {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        // creates disconnected graph with 4 connected subgraphs
        GraphBuilder.create(db)
                .setRelationship("RELATION")
                .setLabel("Label")
                .newLineBuilder().createLine(5)
                .newLineBuilder().createLine(5)
                .newLineBuilder().createLine(5)
                .newLineBuilder().createLine(6);

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);

    }

    @Test
    public void test() throws Exception {

        final DisjointSetStruct struct = new ParallelUnionFind(graph, null)
                .compute(5)
                .getStruct();


        System.out.println(struct.getSetCount());

        System.out.println(struct);
    }

}
