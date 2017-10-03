package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphalgo.exporter.SCCIntArrayExporter;
import org.neo4j.graphalgo.exporter.SCCTarjanExporter;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphalgo.results.SCCResult;
import org.neo4j.graphalgo.results.SCCStreamResult;
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
public class StronglyConnectedComponentsProc {

    public static final String CONFIG_WRITE_PROPERTY = "partitionProperty";
    public static final String CONFIG_CLUSTER = "partition";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    // default algo.scc -> iterative tarjan
    @Procedure(value = "algo.scc", mode = Mode.WRITE)
    @Description("CALL algo.scc(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccDefaultMethod(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return sccIterativeTarjan(label, relationship, config);
    }

    // default algo.scc -> iter tarjan
    @Procedure(value = "algo.scc.stream")
    @Description("CALL algo.scc.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCStreamResult> sccDefaultMethodStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return sccIterativeTarjanStream(label, relationship, config);
    }

    // algo.scc.tarjan
    @Procedure(value = "algo.scc.recursive.tarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.tarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCTarjan tarjan = new SCCTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(Tarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> {
            tarjan.compute();
            builder.withMaxSetSize(tarjan.getMaxSetSize())
                    .withMinSetSize(tarjan.getMinSetSize())
                    .withSetCount(tarjan.getConnectedComponents().size());
        });

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final ObjectArrayList<IntSet> connectedComponents = tarjan.getConnectedComponents();
                tarjan.release();
                new SCCTarjanExporter(api)
                        .withIdMapping(graph)
                        .withWriteProperty(configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER))
                        .write(connectedComponents);
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.tunedTarjan
    @Procedure(value = "algo.scc.recursive.tunedTarjan", mode = Mode.WRITE)
    @Description("CALL algo.scc.recursive.tunedTarjan(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTunedTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCTunedTarjan tarjan = new SCCTunedTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(TunedTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(tarjan::compute);

        builder.withMaxSetSize(tarjan.getMaxSetSize())
                .withMinSetSize(tarjan.getMinSetSize())
                .withSetCount(tarjan.getSetCount());

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new SCCIntArrayExporter(api, graph, log,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(tarjan.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.tunedTarjan.stream
    @Procedure(value = "algo.scc.recursive.tunedTarjan.stream", mode = Mode.WRITE)
    @Description("CALL algo.scc.recursive.tunedTarjan.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, partition")
    public Stream<SCCStreamResult> sccTunedTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        return new SCCTunedTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(TunedTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
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
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        SCCIterativeTarjan tarjan = new SCCIterativeTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(tarjan::compute);

        builder.withSetCount(tarjan.getSetCount())
                .withMinSetSize(tarjan.getMinSetSize())
                .withMaxSetSize(tarjan.getMaxSetSize());

        if (configuration.isWriteFlag()) {
            final int[] connectedComponents = tarjan.getConnectedComponents();
            graph.release();
            tarjan.release();
            builder.timeWrite(() -> {
                new SCCIntArrayExporter(api, graph, log,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(connectedComponents);
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.iterative.stream
    @Procedure(value = "algo.scc.iterative.stream", mode = Mode.WRITE)
    @Description("CALL algo.scc.iterative.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
            "nodeId, partition")
    public Stream<SCCStreamResult> sccIterativeTarjanStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        final SCCIterativeTarjan compute = new SCCIterativeTarjan(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(IterativeTarjan)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute();

        graph.release();

        return compute.resultStream();
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
        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .load(configuration.getGraphImpl());
        loadTimer.stop();

        final MultistepSCC multistep = new MultistepSCC(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getConcurrency(),
                configuration.getNumber("cutoff", 100_000).intValue())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(MultiStep)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(multistep::compute);

        builder.withMaxSetSize(multistep.getMaxSetSize())
                .withMinSetSize(multistep.getMinSetSize())
                .withSetCount(multistep.getSetCount());

        if (configuration.isWriteFlag()) {
            final int[] connectedComponents = multistep.getConnectedComponents();
            graph.release();
            multistep.release();
            builder.timeWrite(() -> {
                new SCCIntArrayExporter(api, graph, log,
                        configuration.get(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(connectedComponents);
            });
        }

        return Stream.of(builder.build());
    }

    // algo.scc.multistep.stream
    @Procedure(value = "algo.scc.multistep.stream")
    @Description("CALL algo.scc.multistep.stream(label:String, relationship:String, {write:true, concurrency:4, cutoff:100000}) YIELD " +
            "nodeId, partition")
    public Stream<SCCStreamResult> multistepStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .load(configuration.getGraphImpl());

        final MultistepSCC multistep = new MultistepSCC(graph, org.neo4j.graphalgo.core.utils.Pools.DEFAULT,
                configuration.getConcurrency(),
                configuration.getNumber("cutoff", 100_000).intValue())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(MultiStep)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        multistep.compute();
        graph.release();
        return multistep.resultStream();
    }

    // algo.scc.forwardBackward.stream
    @Procedure(value = "algo.scc.forwardBackward.stream")
    @Description("CALL algo.scc.forwardBackward.stream(long startNodeId, label:String, relationship:String, {write:true, concurrency:4}) YIELD " +
            "nodeId, partition")
    public Stream<ForwardBackwardScc.Result> fwbwStream(
            @Name(value = "startNodeId", defaultValue = "0") long startNodeId,
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutRelationshipWeights()
                .load(configuration.getGraphImpl());

        final ForwardBackwardScc algo = new ForwardBackwardScc(graph, Pools.DEFAULT,
                configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "SCC(ForwardBackward)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(graph.toMappedNodeId(startNodeId));
        graph.release();
        return algo.resultStream();
    }
}
