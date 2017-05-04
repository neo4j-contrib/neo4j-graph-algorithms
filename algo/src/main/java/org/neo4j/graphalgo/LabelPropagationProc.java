package org.neo4j.graphalgo;

import algo.Pools;
import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.loader.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.loader.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphalgo.impl.LabelPropagationExporter;
import org.neo4j.graphalgo.impl.LabelPropagationStats;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static algo.util.Util.parseDirection;

public final class LabelPropagationProc {

    public static final String CONFIG_ITERATIONS = "iterations";
    public static final String CONFIG_WRITE = "write";
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

        label = emptyToNull(label);
        relationshipType = emptyToNull(relationshipType);
        Direction direction = parseDirection(directionName);

        ProcedureConfiguration configuration = new ProcedureConfiguration(config, log);

        String partitionProperty = configuration.getStringOrNull(
                CONFIG_PARTITION_KEY,
                DEFAULT_PARTITION_KEY);

        LabelPropagationStats.Builder stats = new LabelPropagationStats.Builder()
                .iterations(configuration.getIterations())
                .partitionProperty(partitionProperty)
                .weightProperty(configuration.getProperty());

        HeavyGraph graph = load(
                label,
                relationshipType,
                partitionProperty,
                configuration.getProperty(),
                stats);

        IntDoubleMap labels = compute(direction, configuration.getIterations(), graph, stats);

        if (configuration.isWriteFlag() && partitionProperty != null) {
            write(partitionProperty, graph, labels, stats);
        }

        return Stream.of(stats.build());
    }

    private HeavyGraph load(
            String label,
            String relationshipType,
            String partitionKey,
            String weightKey,
            LabelPropagationStats.Builder stats) {
        long start = System.nanoTime();
        HeavyGraph graph = (HeavyGraph) new GraphLoader(dbAPI)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationshipType)
                .withOptionalRelationshipWeightsFromProperty(weightKey, 1.0d)
                .withOptionalNodeWeightsFromProperty(weightKey, 1.0d)
                .withOptionalNodeProperty(partitionKey, 0.0d)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
        stats.loadMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return graph;
    }

    private IntDoubleMap compute(
            Direction direction,
            int iterations,
            HeavyGraph graph,
            LabelPropagationStats.Builder stats) {
        long start = System.nanoTime();
        IntDoubleMap labels = new LabelPropagation(graph).compute(
                direction,
                iterations);
        stats.computeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                .nodes(labels.size());
        return labels;
    }

    private void write(
            String partitionKey,
            HeavyGraph graph,
            IntDoubleMap labels,
            LabelPropagationStats.Builder stats) {
        long start = System.nanoTime();
        new LabelPropagationExporter(dbAPI, graph, partitionKey).write(labels);
        stats.writeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                .write(true);
    }

    private String emptyToNull(String s) {
        return "".equals(s) ? null : s;
    }
}
