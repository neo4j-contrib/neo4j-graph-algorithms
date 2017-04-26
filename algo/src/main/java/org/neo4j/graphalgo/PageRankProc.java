package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.PageRankAlgo;
import org.neo4j.graphalgo.impl.PageRankExporter;
import org.neo4j.graphalgo.impl.PageRankScore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class PageRankProc {

    public static final String CONFIG_DAMPING = "dampingFactor";
    public static final String CONFIG_ITERATIONS = "iterations";
    public static final String CONFIG_WRITE = "write";
    public static final String CONFIG_SCORE_PROPERTY = "scoreProperty";

    public static final Double DEFAULT_DAMPING = 0.85;
    public static final Integer DEFAULT_ITERATIONS = 20;
    public static final Boolean DEFAULT_WRITE = Boolean.TRUE;
    public static final String DEFAULT_SCORE_PROPERTY = "score";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.pageRank", mode = Mode.WRITE)
    @Description("CALL algo.pageRank(label:String, relationship:String, " +
            "{iterations:5, dampingFactor:0.85, write: true, scoreProperty:'score'}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, property" +
            " - calculates page rank and potentially writes back")
    public Stream<PageRankScore.Stats> pageRank(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        final Graph graph = load(label, relationship, statsBuilder);
        double[] scores = evaluate(graph, config, statsBuilder);
        write(graph, scores, config, statsBuilder);

        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "algo.pageRankStream", mode = Mode.READ)
    @Description("CALL algo.pageRankStream(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85}) " +
            "YIELD node, score - calculates page rank and streams results")
    public Stream<PageRankScore> pageRankStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        final Graph graph = load(label, relationship, statsBuilder);
        double[] scores = evaluate(graph, config, statsBuilder);

        return IntStream.range(0, scores.length)
                .mapToObj(i -> new PageRankScore(
                        api.getNodeById(graph.toOriginalNodeId(i)),
                        scores[i]
                ));
    }

    @Procedure(value = "algo.pageRankStats", mode = Mode.READ)
    @Description("CALL algo.pageRankStats(label:String, relationship:String, " +
            "{iterations:20, dampingFactor:0.85}) " +
            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, property" +
            " - calculates page rank and returns statistics")
    public Stream<PageRankScore.Stats> pageRankStats(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if ((boolean) config.getOrDefault(CONFIG_WRITE, DEFAULT_WRITE)) {
            Map<String, Object> newConfig = new HashMap<>(config);
            newConfig.put(CONFIG_WRITE, Boolean.FALSE);
            config = newConfig;
        }
        return pageRank(label, relationship, config);
    }

    private Graph load(
            String label,
            String relationship,
            PageRankScore.Stats.Builder statsBuilder) {
        long start = System.nanoTime();
        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
        statsBuilder
                .withLoadMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                .withNodes(graph.nodeCount());
        return graph;
    }

    private double[] evaluate(
            Graph graph,
            Map<String, Object> config,
            PageRankScore.Stats.Builder statsBuilder) {
        double dampingFactor = ((Number) config.getOrDefault(
                CONFIG_DAMPING,
                DEFAULT_DAMPING)).doubleValue();

        int iterations = ((Number) config.getOrDefault(
                CONFIG_ITERATIONS,
                DEFAULT_ITERATIONS)).intValue();

        log.debug("Computing page rank with damping of " + dampingFactor + " and " + iterations + " iterations.");

        long start = System.nanoTime();
        double[] result = new PageRankAlgo(
                graph,
                graph,
                graph,
                graph,
                dampingFactor
        ).compute(iterations);
        statsBuilder
                .withComputeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                .withIterations(iterations)
                .withDampingFactor(dampingFactor);
        return result;
    }

    private void write(
            IdMapping idMapping,
            double[] scores,
            Map<String, Object> config,
            final PageRankScore.Stats.Builder statsBuilder) {
        if ((boolean) config.getOrDefault(CONFIG_WRITE, DEFAULT_WRITE)) {
            log.debug("Writing results");
            String propertyName = String.valueOf(config.getOrDefault(
                    CONFIG_SCORE_PROPERTY,
                    DEFAULT_SCORE_PROPERTY));
            long start = System.nanoTime();
            new PageRankExporter(api, idMapping, propertyName).write(scores);
            statsBuilder
                    .withWriteMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                    .withWrite(true)
                    .withProperty(propertyName);
        } else {
            statsBuilder.withWrite(false);
        }
    }
}
