package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.PageRank;
import org.neo4j.graphalgo.impl.PageRankExporter;
import org.neo4j.graphalgo.impl.PageRankScore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class PageRankProc {

    public static final String CONFIG_DAMPING = "dampingFactor";

    public static final Double DEFAULT_DAMPING = 0.85;
    public static final Integer DEFAULT_ITERATIONS = 20;
    public static final String DEFAULT_SCORE_PROPERTY = "pagerank";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.pageRank", mode = Mode.WRITE)
    @Description("CALL algo.pageRank(label:String, relationship:String, " +
            "{iterations:5, dampingFactor:0.85, write: true, writeProperty:'pagerank'}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> pageRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        final Graph graph = load(label, relationship, configuration.getGraphImpl(), statsBuilder);
        double[] scores = evaluate(graph, configuration, statsBuilder);
        write(graph, scores, configuration, statsBuilder);

        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "algo.pageRank.stream", mode = Mode.READ)
    @Description("CALL algo.pageRank.stream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<PageRankScore> pageRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        final Graph graph = load(label, relationship, configuration.getGraphImpl(), statsBuilder);
        double[] scores = evaluate(graph, configuration, statsBuilder);

        return IntStream.range(0, scores.length)
                .mapToObj(i -> new PageRankScore(
                        api.getNodeById(graph.toOriginalNodeId(i)),
                        scores[i]
                ));
    }

    private Graph load(
            String label,
            String relationship,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder) {

        GraphLoader graphLoader = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withExecutorService(Pools.DEFAULT);

        try (ProgressTimer timer = statsBuilder.timeLoad()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

    private double[] evaluate(
            Graph graph,
            ProcedureConfiguration configuration,
            PageRankScore.Stats.Builder statsBuilder) {

        double dampingFactor = configuration.get(CONFIG_DAMPING, DEFAULT_DAMPING);
        int iterations = configuration.getIterations(DEFAULT_ITERATIONS);
        final int batchSize = configuration.getBatchSize();
        log.debug("Computing page rank with damping of " + dampingFactor + " and " + iterations + " iterations.");

        PageRank algo = new PageRank(
                Pools.DEFAULT,
                batchSize,
                graph,
                graph,
                graph,
                graph,
                dampingFactor);

        statsBuilder.timeEval(() -> algo.compute(iterations));

        statsBuilder
                .withIterations(iterations)
                .withDampingFactor(dampingFactor);

        return algo.getPageRank();
    }

    private void write(
            Graph graph,
            double[] scores,
            ProcedureConfiguration configuration,
            final PageRankScore.Stats.Builder statsBuilder) {
        if (configuration.isWriteFlag(true)) {
            log.debug("Writing results");
            String propertyName = configuration.getWriteProperty(DEFAULT_SCORE_PROPERTY);
            int batchSize = configuration.getBatchSize();
            statsBuilder.timeWrite(() -> new PageRankExporter(
                    batchSize,
                    api,
                    graph,
                    graph,
                    propertyName,
                    Pools.DEFAULT)
                    .write(scores));
            statsBuilder
                    .withWrite(true)
                    .withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }
}
