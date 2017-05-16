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
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 *
 * More information in:<br>
 *
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 *
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
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> deltaSteppingStream(
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
