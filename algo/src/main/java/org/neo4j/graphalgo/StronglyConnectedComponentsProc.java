package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.ForwardBackwardScc;
import org.neo4j.graphalgo.impl.MultistepSCCExporter;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphalgo.impl.SCCTarjan;
import org.neo4j.graphalgo.impl.SCCTarjanExporter;
import org.neo4j.graphalgo.results.MultiStepSCCResult;
import org.neo4j.graphalgo.results.SCCResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class StronglyConnectedComponentsProc {

    public static final String CONFIG_WRITE_PROPERTY = "clusterProperty";
    public static final String CONFIG_CLUSTER = "cluster";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.scc", mode = Mode.WRITE)
    @Description("CALL algo.scc(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCTarjan tarjan = new SCCTarjan(graph);
        builder.timeEval(() -> {
            tarjan.compute();
            builder.withMaxSetSize(tarjan.getMaxSetSize())
                    .withMinSetSize(tarjan.getMinSetSize())
                    .withSetCount(tarjan.getConnectedComponents().size());
        });

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new SCCTarjanExporter(api)
                        .withIdMapping(graph)
                        .withWriteProperty(configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER))
                        .write(tarjan.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    @Procedure(value = "algo.scc.multistep", mode = Mode.WRITE)
    @Description("CALL algo.scc.multistep(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "loadMillis, computeMillis, writeMillis")
    public Stream<MultiStepSCCResult> multistep(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        MultiStepSCCResult.Builder builder = MultiStepSCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        loadTimer.stop();

        final MultistepSCC multistep = new MultistepSCC(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getNumber("concurrency", 1).intValue(),
                configuration.getNumber("cutoff", 100_000).intValue());

        builder.timeEval(multistep::compute);

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new MultistepSCCExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        new MultistepSCCExporter.NodeBatch(graph.nodeCount()),
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER),
                        org.neo4j.graphalgo.core.utils.Pools.DEFAULT)
                        .write(multistep.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    @Procedure(value = "algo.scc.multistep.stream")
    @Description("CALL algo.scc.multistep.stream(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "nodeId, clusterId")
    public Stream<MultistepSCC.SCCStreamResult> multistepStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);


        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());

        final MultistepSCC multistep = new MultistepSCC(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getNumber("concurrency", 1).intValue(),
                configuration.getNumber("cutoff", 100_000).intValue());

        multistep.compute();

        return multistep.resultStream();
    }

    @Procedure(value = "algo.scc.fwbw.stream")
    @Description("CALL algo.scc.fwbw.stream(long startNodeId, label:String, relationship:String, {write:true, concurrency:4}) YIELD " +
            "nodeId")
    public Stream<ForwardBackwardScc.Result> fwbwStream(
            @Name(value = "startNodeId", defaultValue = "0") long startNodeId,
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());

        return new ForwardBackwardScc(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getConcurrency(1))
                .compute(graph.toMappedNodeId(startNodeId))
                .resultStream();
    }

}
