package org.neo4j.graphalgo;

import algo.Pools;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphalgo.impl.LabelPropagationExporter;
import org.neo4j.graphalgo.impl.LabelPropagationStats;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static java.lang.String.format;

public final class LabelPropagationProc {

    public static final String CONFIG_WEIGHT_KEY = "weightProperty";
    public static final String CONFIG_PARTITION_KEY = "partitionProperty";
    public static final Integer DEFAULT_ITERATIONS = 1;
    public static final Boolean DEFAULT_WRITE = Boolean.TRUE;
    public static final String DEFAULT_WEIGHT_KEY = "weight";
    public static final String DEFAULT_PARTITION_KEY = "partition";

    @Context
    public GraphDatabaseAPI dbAPI;

    @Procedure(name = "algo.labelPropagation", mode = Mode.WRITE)
    @Description("CALL algo.labelPropagation(" +
            "label:String, relationship:String, direction:String, " +
            "{iterations:1, weightProperty:'weight', partitionProperty:'partition', write:true}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, write, weightProperty, partitionProperty - " +
            "simple label propagation kernel")
    public Stream<LabelPropagationStats> labelPropagation(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionName,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationshipType);

        final Direction direction = parseDirection(directionName);
        final int iterations = configuration.getIterations(DEFAULT_ITERATIONS);
        final int batchSize = configuration.getBatchSize();
        final String partitionProperty = configuration.getStringOrNull(CONFIG_PARTITION_KEY, DEFAULT_PARTITION_KEY);
        final String weightProperty = configuration.getStringOrNull(CONFIG_WEIGHT_KEY, DEFAULT_WEIGHT_KEY);

        LabelPropagationStats.Builder stats = new LabelPropagationStats.Builder()
                .iterations(iterations)
                .partitionProperty(partitionProperty)
                .weightProperty(weightProperty);

        HeavyGraph graph = load(
                configuration.getNodeLabelOrQuery(),
                configuration.getRelationshipOrQuery(),
                partitionProperty,
                weightProperty,
                stats);

        IntDoubleMap labels = compute(direction, iterations, batchSize, graph, stats);

        stats.nodes(labels.size());

        if (configuration.isWriteFlag(DEFAULT_WRITE) && partitionProperty != null) {
            write(batchSize, partitionProperty, graph, labels, stats);
        }

        return Stream.of(stats.build());
    }

    private HeavyGraph load(
            String label,
            String relationshipType,
            String partitionKey,
            String weightKey,
            LabelPropagationStats.Builder stats) {

        try (ProgressTimer timer = stats.timeLoad()) {
            return (HeavyGraph) new GraphLoader(dbAPI)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationshipType)
                    .withOptionalRelationshipWeightsFromProperty(weightKey, 1.0d)
                    .withOptionalNodeWeightsFromProperty(weightKey, 1.0d)
                    .withOptionalNodeProperty(partitionKey, 0.0d)
                    .withExecutorService(Pools.DEFAULT)
                    .load(HeavyGraphFactory.class);
        }
    }

    private IntDoubleMap compute(
            Direction direction,
            int iterations,
            int batchSize,
            HeavyGraph graph,
            LabelPropagationStats.Builder stats) {
        try (ProgressTimer timer = stats.timeEval()) {
            ExecutorService pool = batchSize > 0 ? Pools.DEFAULT : null;
            return new LabelPropagation(graph, pool).compute(
                    direction,
                    iterations,
                    Math.max(1, batchSize)
            );
        }
    }

    private void write(
            int batchSize,
            String partitionKey,
            HeavyGraph graph,
            IntDoubleMap labels,
            LabelPropagationStats.Builder stats) {
        stats.write(true);
        try (ProgressTimer timer = stats.timeWrite()) {
            new LabelPropagationExporter(
                    batchSize,
                    dbAPI,
                    graph,
                    new BatchLabels(labels),
                    partitionKey,
                    Pools.DEFAULT)
                    .write(labels);
        }
    }

    private static final Direction[] ALLOWED_DIRECTION = Arrays
            .stream(Direction.values())
            .filter(d -> d != Direction.BOTH)
            .toArray(Direction[]::new);

    private static Direction parseDirection(String directionString) {
        if (null == directionString) {
            return Direction.OUTGOING;
        }
        Direction direction;
        try {
            direction = Direction.valueOf(directionString.toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    format(
                            "Cannot convert value '%s' to Direction. Legal values are '%s'.",
                            directionString,
                            Arrays.toString(ALLOWED_DIRECTION)
                    )
            );
        }
        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                    format(
                            "Direction BOTH is not allowed. Legal values are '%s'.",
                            Arrays.toString(ALLOWED_DIRECTION)
                    )
            );
        }
        return direction;
    }
}


final class BatchLabels implements BatchNodeIterable {
    private IntDoubleMap data;

    BatchLabels(IntDoubleMap data) {
        this.data = data;
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        int numberOfBatches = ParallelUtil.threadSize(batchSize, data.size());
        Iterator<IntCursor> keys = data.keys().iterator();
        PrimitiveIntIterable[] iterators = new PrimitiveIntIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> new SegmentIterable(keys, batchSize));
        return Arrays.asList(iterators);
    }

    private static final class SegmentIterable implements PrimitiveIntIterable, PrimitiveIntIterator {
        private int[] keys;
        private int current;
        private int limit;

        private SegmentIterable(
                Iterator<IntCursor> data,
                int limit) {
            int[] keys = new int[limit];
            int i;
            for (i = 0; i < limit && data.hasNext(); i++) {
                keys[i] = data.next().value;
            }
            this.limit = i;
            this.keys = keys;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            current = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public int next() {
            return keys[current++];
        }
    }
}
