package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.LabelPropagationExporter;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphalgo.results.LabelPropagationStats;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
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

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.labelPropagation", mode = Mode.WRITE)
    @Description("CALL algo.labelPropagation(" +
            "label:String, relationship:String, direction:String, " +
            "{iterations:1, weightProperty:'weight', partitionProperty:'partition', write:true, concurrency:4}) " +
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
        final int concurrency = configuration.getConcurrency();
        final String partitionProperty = configuration.getString(CONFIG_PARTITION_KEY, DEFAULT_PARTITION_KEY);
        final String weightProperty = configuration.getString(CONFIG_WEIGHT_KEY, DEFAULT_WEIGHT_KEY);

        LabelPropagationStats.Builder stats = new LabelPropagationStats.Builder()
                .iterations(iterations)
                .partitionProperty(partitionProperty)
                .weightProperty(weightProperty);

        HeavyGraph graph = load(
                configuration.getNodeLabelOrQuery(),
                configuration.getRelationshipOrQuery(),
                direction,
                partitionProperty,
                weightProperty,
                batchSize,
                concurrency,
                stats);

        IntDoubleMap labels = compute(direction, iterations, batchSize, concurrency, graph, stats);

        stats.nodes(labels.size());

        if (configuration.isWriteFlag(DEFAULT_WRITE) && partitionProperty != null) {
            write(concurrency, partitionProperty, graph, labels, stats);
        }

        return Stream.of(stats.build());
    }

    private HeavyGraph load(
            String label,
            String relationshipType,
            Direction direction,
            String partitionKey,
            String weightKey,
            int batchSize,
            int concurrency,
            LabelPropagationStats.Builder stats) {

        try (ProgressTimer timer = stats.timeLoad()) {
            return (HeavyGraph) new GraphLoader(dbAPI, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationshipType)
                    .withOptionalRelationshipWeightsFromProperty(weightKey, 1.0d)
                    .withOptionalNodeWeightsFromProperty(weightKey, 1.0d)
                    .withOptionalNodeProperty(partitionKey, 0.0d)
                    .withDirection(direction)
                    .withBatchSize(batchSize)
                    .withConcurrency(concurrency)
                    .load(HeavyGraphFactory.class);
        }
    }

    private IntDoubleMap compute(
            Direction direction,
            int iterations,
            int batchSize,
            int concurrency,
            HeavyGraph graph,
            LabelPropagationStats.Builder stats) {
        try (ProgressTimer timer = stats.timeEval()) {
            ExecutorService pool = batchSize > 0 ? Pools.DEFAULT : null;
            batchSize = Math.max(1, batchSize);
            final LabelPropagation labelPropagation = new LabelPropagation(graph, batchSize, concurrency, pool);
            final IntDoubleMap result = labelPropagation
                    .withProgressLogger(ProgressLogger.wrap(log, "LabelPropagation"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(direction, iterations);
            labelPropagation.release();
            graph.release();
            return result;
        }
    }

    private void write(
            int concurrency,
            String partitionKey,
            HeavyGraph graph,
            IntDoubleMap labels,
            LabelPropagationStats.Builder stats) {
        stats.write(true);
        try (ProgressTimer timer = stats.timeWrite()) {
            new LabelPropagationExporter(dbAPI, graph, log, partitionKey, Pools.DEFAULT)
                    .withConcurrency(concurrency)
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
