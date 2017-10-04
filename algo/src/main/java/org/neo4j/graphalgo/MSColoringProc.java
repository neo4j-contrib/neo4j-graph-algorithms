package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.exporter.AtomicIntArrayExporter;
import org.neo4j.graphalgo.impl.MSColoring;
import org.neo4j.graphalgo.results.UnionFindResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class MSColoringProc {

    public static final String CONFIG_CLUSTER_PROPERTY = "partitionProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.unionFind.mscoloring", mode = Mode.WRITE)
    @Description("CALL algo.unionFind.mscoloring(label:String, relationship:String, " +
            "{property:'weight', threshold:0.42, defaultValue:1.0, write: true, partitionProperty:'partition', concurrency:4}) " +
            "YIELD nodes, setCount, loadMillis, computeMillis, writeMillis")
    public Stream<UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        UnionFindResult.Builder builder = UnionFindResult.builder();

        // loading
        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = load(configuration);
        }

        // evaluation
        final AtomicIntegerArray struct;
        try (ProgressTimer timer = builder.timeEval()) {
            struct = evaluate(graph, configuration);
        }

        if (configuration.isWriteFlag()) {
            // write back
            builder.timeWrite(() ->
                    write(graph, struct, configuration));
        }

        return Stream.of(builder.build());
    }

    @Procedure(value = "algo.unionFind.mscoloring.stream")
    @Description("CALL algo.unionFind.mscoloring.stream(label:String, relationship:String, " +
            "{property:'propertyName', threshold:0.42, defaultValue:1.0, concurrency:4) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<MSColoring.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        // loading
        final Graph graph = load(configuration);

        // evaluation
        return new MSColoring(graph, Pools.DEFAULT, configuration.getConcurrency())
                .compute()
                .resultStream();
    }

    private Graph load(ProcedureConfiguration config) {
        return new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(config.getNodeLabelOrQuery())
                .withOptionalRelationshipType(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getProperty(),
                        config.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .load(config.getGraphImpl());
    }

    private AtomicIntegerArray evaluate(Graph graph, ProcedureConfiguration config) {
        return new MSColoring(graph, Pools.DEFAULT, config.getConcurrency())
                .compute()
                .getColors();
    }

    private void write(Graph graph, AtomicIntegerArray struct, ProcedureConfiguration configuration) {
        log.debug("Writing results");
        new AtomicIntArrayExporter(
                api,
                graph,
                log,
                configuration.get(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY),
                Pools.DEFAULT
        ).write(struct);
    }
}
