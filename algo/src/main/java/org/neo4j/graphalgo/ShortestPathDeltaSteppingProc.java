package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathDeltaSteppingProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure("algo.deltaStepping.stream")
    @Description("CALL algo.deltaStepping.stream(startNode:Node, propertyName:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, distance} from start to end (inclusive)")
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> deltaStepping(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(Double.MAX_VALUE))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        return new ShortestPathDeltaStepping(graph, delta)
                .withExecutorService(Pools.DEFAULT)
                .compute(startNode.getId())
                .resultStream();
    }
}
