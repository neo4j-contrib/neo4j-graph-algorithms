package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.AllShortestPaths;
import org.neo4j.graphalgo.impl.MSBFSAllShortestPaths;
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
public class AllShortestPathsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.allShortestPaths.stream")
    @Description("CALL algo.allShortestPaths.stream(weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0, concurrency:4}) " +
            "YIELD sourceNodeId, targetNodeId, distance - yields a stream of {sourceNodeId, targetNodeId, distance}")
    public Stream<AllShortestPaths.Result> allShortestPathsStream(
            @Name(value = "propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        // use MSBFS ASP if no weightProperty is set
        if (null == propertyName || propertyName.isEmpty()) {

            final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withDirection(Direction.OUTGOING)
                    .load(configuration.getGraphImpl());

            return new MSBFSAllShortestPaths(graph, configuration.getConcurrency(), Pools.DEFAULT)
                    .withProgressLogger(ProgressLogger.wrap(log, "AllShortestPaths(MultiSource)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .resultStream();
        }

        // weighted ASP otherwise
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        return new AllShortestPaths(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withProgressLogger(ProgressLogger.wrap(log, "AllShortestPaths"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .resultStream();
    }
}
