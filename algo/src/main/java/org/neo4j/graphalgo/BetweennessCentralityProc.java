package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.BetweennessCentrality;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class BetweennessCentralityProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.betweenness")
    @Description("CALL algo.betweenness(label:String, relationship:String) YIELD nodeId, centrality - yields centrality for each node")
    public Stream<BetweennessCentrality.Result> betweenness(
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
}
