package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphalgo.results.SCCResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class StronglyConnectedComponentsProc {

    public static final String CONFIG_WRITE_PROPERTY = "partitionProperty";
    public static final String CONFIG_CLUSTER = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    // default algo.scc -> tarjan
    @Procedure(value = "algo.scc", mode = Mode.WRITE)
    @Description("CALL algo.scc(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccDefaultMethod(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return sccTarjan(label, relationship, config);
    }

    // algo.scc.tarjan
    @Procedure(value = "algo.scc.tarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.tarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
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
                .withDirection(Direction.OUTGOING)
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

    // algo.scc.tunedTarjan
    @Procedure(value = "algo.scc.tunedTarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.tunedTarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTunedTarjan(
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
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCTunedTarjan tarjan = new SCCTunedTarjan(graph);

        builder.timeEval(tarjan::compute);

        builder.withMaxSetSize(tarjan.getMaxSetSize())
                .withMinSetSize(tarjan.getMinSetSize())
                .withSetCount(tarjan.getSetCount());

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new ArrayBasedSCCExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        graph,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER),
                        org.neo4j.graphalgo.core.utils.Pools.DEFAULT)
                        .write(tarjan.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.tunedTarjan.stream
    @Procedure(value = "algo.scc.tunedTarjan.stream", mode = Mode.WRITE)
    @Description("CALL algo.scc.tunedTarjan.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, cluster")
    public Stream<SCCStreamResult> sccTunedTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        return new SCCTunedTarjan(graph)
                .compute()
                .resultStream();
    }

    // algo.scc.iterative
    @Procedure(value = "algo.scc.iterative", mode = Mode.WRITE)
    @Description("CALL algo.scc.iterative(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccIterativeTarjan(
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
                    .withDirection(Direction.OUTGOING)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCIterativeTarjan tarjan = new SCCIterativeTarjan(graph);

        builder.timeEval(tarjan::compute);

        builder.withSetCount(tarjan.getSetCount())
                .withMinSetSize(tarjan.getMinSetSize())
                .withMaxSetSize(tarjan.getMaxSetSize());

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new ArrayBasedSCCExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        graph,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER),
                        org.neo4j.graphalgo.core.utils.Pools.DEFAULT)
                        .write(tarjan.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.iterative.stream
    @Procedure(value = "algo.scc.iterative.stream", mode = Mode.WRITE)
    @Description("CALL algo.scc.iterative.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, cluster")
    public Stream<SCCStreamResult> sccIterativeTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withDirection(Direction.OUTGOING)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());

        return new SCCIterativeTarjan(graph)
                .compute()
                .resultStream();
    }


    // algo.scc.multistep
    @Procedure(value = "algo.scc.multistep", mode = Mode.WRITE)
    @Description("CALL algo.scc.multistep(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> multistep(
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

        final MultistepSCC multistep = new MultistepSCC(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getNumber("concurrency", 1).intValue(),
                configuration.getNumber("cutoff", 100_000).intValue());

        builder.timeEval(multistep::compute);

        builder.withMaxSetSize(multistep.getMaxSetSize())
                .withMinSetSize(multistep.getMinSetSize())
                .withSetCount(multistep.getSetCount());

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new ArrayBasedSCCExporter(
                        configuration.getBatchSize(),
                        api,
                        graph,
                        graph,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER),
                        org.neo4j.graphalgo.core.utils.Pools.DEFAULT)
                        .write(multistep.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.multistep.stream
    @Procedure(value = "algo.scc.multistep.stream")
    @Description("CALL algo.scc.multistep.stream(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "nodeId, cluster")
    public Stream<SCCStreamResult> multistepStream(
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

    // algo.scc.forwardBackward.stream
    @Procedure(value = "algo.scc.forwardBackward.stream")
    @Description("CALL algo.scc.forwardBackward.stream(long startNodeId, label:String, relationship:String, {write:true, concurrency:4}) YIELD " +
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
