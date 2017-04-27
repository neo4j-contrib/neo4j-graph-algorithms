package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
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

    public static final String CONFIG_WRITE = "write";
    public static final String CONFIG_STATS = "stats";
    public static final String CONFIG_WRITE_PROPERTY = "writeProperty";
    public static final String CONFIG_WRITE_PROPERTY_DEFAULT = "centrality";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.betweennessStream")
    @Description("CALL algo.betweennessStream(label:String, relationship:String) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweennessStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship) {

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);

        return new BetweennessCentrality(graph)
                .compute()
                .resultStream();
    }

    @Procedure(value = "algo.betweenness", mode = Mode.WRITE)
    @Description("CALL algo.betweenness(label:String, relationship:String, {write:true, writeProperty:'centrality', stats:true}) YIELD " +
            "loadDuration, evalDuration, writeDuration, nodeCount, [minCentrality, maxCentrality, meanCentrality] - yields status of evaluation")
    public Stream<BetweennessCentralityProcResult> betweenness(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {


        final BetweennessCentralityProcResult.Builder builder =
                BetweennessCentralityProcResult.builder();

        ProgressTimer timer = ProgressTimer.start(builder::withLoadDuration);
        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);
        timer.stop();

        builder.withNodeCount(graph.nodeCount());

        timer = ProgressTimer.start(builder::withEvalDuration);
        final BetweennessCentrality bc = new BetweennessCentrality(graph)
                .compute();
        timer.stop();

        if ((boolean) config.getOrDefault(CONFIG_WRITE, Boolean.FALSE)) {
            timer = ProgressTimer.start(builder::withWriteDuration);
            new BetweennessCentrality.BCExporter(api)
                    .withTargetProperty((String) config.getOrDefault(CONFIG_WRITE_PROPERTY, CONFIG_WRITE_PROPERTY_DEFAULT))
                    .write(bc);
            timer.stop();
        }

        if ((boolean) config.getOrDefault(CONFIG_STATS, Boolean.FALSE)) {
            computeStats(builder, bc);
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
