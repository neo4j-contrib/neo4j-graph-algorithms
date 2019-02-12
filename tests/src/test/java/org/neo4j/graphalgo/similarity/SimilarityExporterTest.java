package org.neo4j.graphalgo.similarity;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class SimilarityExporterTest {
    @Rule
    public final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final MethodType CTOR_METHOD = MethodType.methodType(
            void.class,
            GraphDatabaseAPI.class,
            Log.class,
            String.class,
            String.class,
            int.class);

    private static final String RELATIONSHIP_TYPE = "SIMILAR";
    private static final String PROPERTY_NAME = "score";
    private SimilarityExporter exporter;
    private GraphDatabaseAPI api;
    private Class<? extends SimilarityExporter> similarityExporterFactory;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{SequentialSimilarityExporter.class, "Sequential"},
                new Object[]{ParallelSimilarityExporter.class, "Parallel"}
        );
    }

    @Before
    public void setup() {
        api = DB.getGraphDatabaseAPI();
    }

    public SimilarityExporterTest(Class<? extends SimilarityExporter> similarityExporterFactory,
                                  String ignoreParamOnlyForTestNaming) throws Throwable {

        this.similarityExporterFactory = similarityExporterFactory;
    }

    public SimilarityExporter load(Class<? extends SimilarityExporter> factoryType, int nodeCount) throws Throwable {
        final MethodHandle constructor = findConstructor(factoryType);
        return (SimilarityExporter) constructor.invoke(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME, nodeCount);
    }

    private MethodHandle findConstructor(Class<?> factoryType) {
        try {
            return LOOKUP.findConstructor(factoryType, CTOR_METHOD);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createNothing() throws Throwable {
        int nodeCount = 2;
        createNodes(api, nodeCount);
        exporter = load(similarityExporterFactory, nodeCount);

        Stream<SimilarityResult> similarityPairs = Stream.empty();

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(0, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);
            assertThat(allRelationships, hasSize(0));
        }
    }

    @Test
    public void createOneRelationship() throws Throwable {
        int nodeCount = 2;
        createNodes(api, nodeCount);
        exporter = load(similarityExporterFactory, nodeCount);

        Stream<SimilarityResult> similarityPairs = Stream.of(new SimilarityResult(0, 1, -1, -1, -1, 0.5));

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(1, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);
            assertThat(allRelationships, hasSize(1));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
        }
    }

    @Test
    public void multipleBatches() throws Throwable {
        int nodeCount = 4;
        createNodes(api, nodeCount);
        exporter = load(similarityExporterFactory, nodeCount);

        SimilarityExporter exporter = new SequentialSimilarityExporter(api, NullLog.getInstance(), RELATIONSHIP_TYPE, PROPERTY_NAME, 4);

        Stream<SimilarityResult> similarityPairs = Stream.of(
                new SimilarityResult(0, 1, -1, -1, -1, 0.5),
                new SimilarityResult(2, 3, -1, -1, -1, 0.7)
        );

        int batches = exporter.export(similarityPairs, 1);
        assertEquals(2, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);

            assertThat(allRelationships, hasSize(2));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(2, 3, 0.7)));
        }
    }

    @Test
    public void smallerThanBatchSize() throws Throwable {
        int nodeCount = 5;
        createNodes(api, nodeCount);
        exporter = load(similarityExporterFactory, nodeCount);

        Stream<SimilarityResult> similarityPairs = Stream.of(
                new SimilarityResult(0, 1, -1, -1, -1, 0.5),
                new SimilarityResult(1, 2, -1, -1, -1, 0.6),
                new SimilarityResult(2, 3, -1, -1, -1, 0.7),
                new SimilarityResult(3, 4, -1, -1, -1, 0.8)
        );

        int batches = exporter.export(similarityPairs, 10);
        assertEquals(1, batches);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);

            assertThat(allRelationships, hasSize(4));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(1, 2, 0.6)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(2, 3, 0.7)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(3, 4, 0.8)));
        }
    }

    @Test
    public void disconnectedUpdates() throws Throwable {
        int nodeCount = 6;
        createNodes(api, nodeCount);
        exporter = load(similarityExporterFactory, nodeCount);

        Stream<SimilarityResult> similarityPairs = Stream.of(
                new SimilarityResult(0, 1, -1, -1, -1, 0.5),
                new SimilarityResult(2, 3, -1, -1, -1, 0.7),
                new SimilarityResult(4, 5, -1, -1, -1, 0.9)
        );

        exporter.export(similarityPairs, 10);

        try (Transaction tx = api.beginTx()) {
            List<SimilarityRelationship> allRelationships = getSimilarityRelationships(api);

            assertThat(allRelationships, hasSize(3));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(0, 1, 0.5)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(2, 3, 0.7)));
            assertThat(allRelationships, hasItems(new SimilarityRelationship(4, 5, 0.9)));
        }
    }

    private List<SimilarityRelationship> getSimilarityRelationships(GraphDatabaseAPI api) {
        return api.getAllRelationships().stream()
                .map(relationship -> new SimilarityRelationship(relationship.getStartNodeId(), relationship.getEndNodeId(), (double)relationship.getProperty(PROPERTY_NAME)))
                .collect(Collectors.toList());
    }

    static class SimilarityRelationship {
        private final long startNodeId;
        private final long endNodeId;
        private final double property;

        SimilarityRelationship(long startNodeId, long endNodeId, double property) {
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.property = property;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimilarityRelationship that = (SimilarityRelationship) o;
            return startNodeId == that.startNodeId &&
                    endNodeId == that.endNodeId &&
                    Double.compare(that.property, property) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(startNodeId, endNodeId, property);
        }
    }

    private void createNodes(GraphDatabaseAPI api, int nodeCount) {
        try (Transaction tx = api.beginTx()) {
            for(int i = 0; i < nodeCount; i++) {
                api.createNode();
            }
            tx.success();
        }
    }
}