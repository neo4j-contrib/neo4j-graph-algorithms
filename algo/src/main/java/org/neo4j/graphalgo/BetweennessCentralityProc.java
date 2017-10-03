package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.exporter.AtomicBigDoubleArrayExporter;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphalgo.exporter.AtomicDoubleArrayExporter;
import org.neo4j.graphalgo.exporter.DoubleArrayExporter;
import org.neo4j.graphalgo.results.BetweennessCentralityProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class BetweennessCentralityProc {


    public static final String DEFAULT_TARGET_PROPERTY = "centrality";
    public static final Direction DEFAULT_DIRECTION = Direction.OUTGOING;


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(value = "algo.betweenness.exp1.stream")
    @Description("CALL algo.betweenness.exp1.stream(label:String, relationship:String, {direction:'out', scaleFactor:100000}) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessSuccessorBrandesStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        final BetweennessCentralitySuccessorBrandes algo = new BetweennessCentralitySuccessorBrandes(graph,
                configuration.getNumber("scaleFactor", 100_000).intValue(),
                Pools.DEFAULT)
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality"))
                .withDirection(Direction.OUTGOING)
                .compute();

        graph.release();

        return algo.resultStream();
    }

    /**
     * Procedure accepts {in, incoming, <, out, outgoing, >, both, <>} as direction
     */
    @Procedure(value = "algo.betweenness.stream")
    @Description("CALL algo.betweenness.stream(label:String, relationship:String, {direction:'out', concurrency :4})" +
                 "YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                .load(configuration.getGraphImpl());

        if (configuration.getConcurrency(-1) > 0) {
            final ParallelBetweennessCentrality algo = new ParallelBetweennessCentrality(graph,
                    configuration.getNumber("scaleFactor", 100_000).intValue(),
                    Pools.DEFAULT,
                    configuration.getConcurrency())
                    .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                    .compute();
            graph.release();
            return algo.resultStream();
        }

        final BetweennessCentrality compute = new BetweennessCentrality(graph)
                .withDirection(configuration.getDirection(DEFAULT_DIRECTION))
                .compute();
        graph.release();
        return compute.resultStream();
    }

    @Procedure(value = "algo.betweenness.exp1", mode = Mode.WRITE)
    @Description("CALL algo.betweenness.exp1(label:String, relationship:String, " +
            "{direction:'out', write:true, writeProperty:'centrality', stats:true, scaleFactor:100000}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality] - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweennessSucessorBrandes(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(Direction.OUTGOING)
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        final BetweennessCentralitySuccessorBrandes bc = new BetweennessCentralitySuccessorBrandes(
                graph,
                configuration.getNumber("scaleFactor", 100_000).doubleValue(),
                Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
            }
        });

        final AtomicDoubleArray centrality = bc.getCentrality();
        bc.release();
        graph.release();

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new AtomicDoubleArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(centrality);

            });
        }

        return Stream.of(builder.build());
    }


    @Procedure(value = "algo.betweenness", mode = Mode.WRITE)
    @Description("CALL algo.betweenness(label:String, relationship:String, {direction:'out',write:true, writeProperty:'centrality', stats:true, concurrency:4}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweenness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        if (configuration.getConcurrency(-1) > 1) {
            return computeBetweennessParallel(label, relationship, configuration);
        } else {
            return computeBetweenness(label, relationship, configuration);
        }
    }

    public Stream<BetweennessCentralityProcResult> computeBetweenness(
            String label,
            String relationship,
            ProcedureConfiguration configuration) {

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(configuration.getDirection(Direction.OUTGOING))
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());
        final BetweennessCentrality bc = new BetweennessCentrality(graph)
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality(sequential)"))
                .withDirection(configuration.getDirection(Direction.OUTGOING));

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
            }
        });

        final double[] centrality = bc.getCentrality();
        bc.release();
        graph.release();

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new DoubleArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(centrality);
            });
        }

        return Stream.of(builder.build());
    }

    public Stream<BetweennessCentralityProcResult> computeBetweennessParallel(
            String label,
            String relationship,
            ProcedureConfiguration configuration) {

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .withDirection(configuration.getDirection(Direction.OUTGOING))
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());

        final ParallelBetweennessCentrality bc = new ParallelBetweennessCentrality(
                graph,
                configuration.getNumber("scaleFactor", 100_000).doubleValue(),
                Pools.DEFAULT,
                configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality(parallel)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withDirection(configuration.getDirection(Direction.OUTGOING));

        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc.getCentrality());
            }
        });

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final AtomicDoubleArray centrality = bc.getCentrality();
                bc.release();
                graph.release();
                new AtomicDoubleArrayExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(centrality);
            });
        }

        return Stream.of(builder.build());
    }

    private void computeStats(BetweennessCentralityProcResult.Builder builder, double[] centrality) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        for (int i = centrality.length - 1; i >= 0; i--) {
            final double c = centrality[i];
            if (c < min) {
                min = c;
            }
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        builder.withCentralityMax(max)
                .withCentralityMin(min)
                .withCentralitySum(sum);
    }

    private void computeStats(BetweennessCentralityProcResult.Builder builder, AtomicDoubleArray centrality) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        for (int i = centrality.length() - 1; i >= 0; i--) {
            final double c = centrality.get(i);
            if (c < min) {
                min = c;
            }
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        builder.withCentralityMax(max)
                .withCentralityMin(min)
                .withCentralitySum(sum);
    }
}
