package org.neo4j.graphalgo;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.ShortestPaths;
import org.neo4j.graphalgo.exporter.IntDoubleMapExporter;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathsProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.shortestPaths.stream")
    @Description("CALL algo.shortestPaths.stream(startNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPaths.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withLog(log)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());

        final ShortestPaths algo = new ShortestPaths(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(startNode.getId());
        graph.release();
        return algo.resultStream();
    }

    @Procedure(value = "algo.shortestPaths", mode = Mode.WRITE)
    @Description("CALL algo.shortestPaths(startNode:Node, weightProperty:String" +
            "{write:true, targetProperty:'path', nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount, targetProperty - yields nodeCount, totalCost, loadDuration, evalDuration")
    public Stream<ShortestPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        ShortestPathResult.Builder builder = ShortestPathResult.builder();

        ProgressTimer load = builder.timeLoad();
        final Graph graph = new GraphLoader(api)
                .withLog(log)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());
        load.stop();

        final ShortestPaths algorithm = new ShortestPaths(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final IntDoubleMap shortestPaths = algorithm.getShortestPaths();
                algorithm.release();
                graph.release();
                new IntDoubleMapExporter(api, graph, log, configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .write(shortestPaths);
            });
        }

        return Stream.of(builder.build());
    }

}
