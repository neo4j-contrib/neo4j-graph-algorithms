package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.ShortestPaths;
import org.neo4j.graphalgo.exporter.IntDoubleMapExporter;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
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

    @Procedure("algo.shortestPaths.stream")
    @Description("CALL algo.shortestPaths.stream(startNodeId:long, propertyName:String" +
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

        return new ShortestPaths(graph)
                .withLog(log)
                .compute(startNode.getId())
                .resultStream();
    }

    @Procedure(value = "algo.shortestPaths", mode = Mode.WRITE)
    @Description("CALL algo.shortestPaths(startNodeId:long, propertyName:String" +
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
                .withLog(log);

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                new IntDoubleMapExporter(api, graph, log, configuration.getWriteProperty(), Pools.DEFAULT)
                        .write(algorithm.getShortestPaths());
            });
        }

        return Stream.of(builder.build());
    }

}
