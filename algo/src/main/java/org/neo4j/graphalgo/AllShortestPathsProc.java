package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.AllShortestPaths;
import org.neo4j.graphalgo.impl.MSBFSAllShortestPaths;
import org.neo4j.graphdb.Direction;
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

    @Procedure("algo.allShortestPaths.stream")
    @Description("CALL algo.allShortestPaths.stream(propertyName:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD sourceNodeId, targetNodeId, distance - yields a stream of {sourceNodeId, targetNodeId, distance}")
    public Stream<AllShortestPaths.Result> allShortestPathsStream(
            @Name(value = "propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        // use MSBFS ASP if no weightProperty is set
        if (null == propertyName || propertyName.isEmpty()) {

            final Graph graph = new GraphLoader(api)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withDirection(Direction.OUTGOING)
                    .withExecutorService(Pools.DEFAULT)
                    .load(configuration.getGraphImpl());

            return new MSBFSAllShortestPaths(graph, Pools.DEFAULT)
                    .resultStream();
        }

        // weighted ASP otherwise
        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(1.0))
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .load(configuration.getGraphImpl());



        return new AllShortestPaths(graph, Pools.DEFAULT, configuration.getConcurrency())
                .withLog(log)
                .resultStream();
    }
}
