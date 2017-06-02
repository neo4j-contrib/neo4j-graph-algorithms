package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.BetweennessCentrality;
import org.neo4j.graphalgo.results.BetweennessCentralityProcResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class BetweennessCentralityProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.betweenness.stream")
    @Description("CALL algo.betweenness.stream(label:String, relationship:String) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .load(configuration.getGraphImpl());

        return new BetweennessCentrality(graph)
                .compute()
                .resultStream();
    }

    @Procedure(value = "algo.betweenness", mode = Mode.WRITE)
    @Description("CALL algo.betweenness(label:String, relationship:String, {write:true, writeProperty:'centrality', stats:true}) YIELD " +
            "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, meanCentrality] - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweenness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutNodeProperties()
                    .load(configuration.getGraphImpl());
        }

        builder.withNodeCount(graph.nodeCount());
        final BetweennessCentrality bc = new BetweennessCentrality(graph);
        builder.timeEval(() -> {
            bc.compute();
            if (configuration.isStatsFlag()) {
                computeStats(builder, bc);
            }
        });

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() ->
                    new BetweennessCentrality.BCExporter(api)
                            .withTargetProperty(configuration.getWriteProperty())
                            .write(bc));
        }

        return Stream.of(builder.build());
    }

    private void computeStats(BetweennessCentralityProcResult.Builder builder, BetweennessCentrality bc) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        double[] centrality = bc.getCentrality();
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
}
