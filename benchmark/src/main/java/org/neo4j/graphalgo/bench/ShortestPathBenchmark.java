package org.neo4j.graphalgo.bench;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 * @author mknblch
 */
public class ShortestPathBenchmark {

    private static GraphDatabaseAPI db;

    public static void setup() {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

    }
}
