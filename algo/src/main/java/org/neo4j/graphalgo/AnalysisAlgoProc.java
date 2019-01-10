package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.AnalysisAlgo;
import org.neo4j.graphalgo.impl.AnalysisAlgoV2;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

public class AnalysisAlgoProc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(value = "fk.algo.analysis_algo", mode = Mode.WRITE)
    @Description("CALL fk.algo.analysis_algo(companyId:String," +
            " fileName:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName'}) " +
            "")
    public void analysis_algo(
            @Name("companyId") String companyId,
            @Name("fileName") String fileName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
//        final Graph graph;
//        final String routeWeight = configuration.getString("routeWeight", "distance");
//        final String nodeLat = configuration.getString("nodeLat", "latitude");
//        final String nodeLon = configuration.getString("nodeLon", "longitude");
//
//        // load
//        final GraphLoader laneGraphLoader = new GraphLoader(api, Pools.DEFAULT)
//                .init(log, 'City', 'ROUTE', configuration)
//                .withRelationshipWeightsFromProperty('distance', 99999999.0);
//        // use undirected traversal if direction is BOTH
//        laneGraphLoader.asUndirected(true);
//        graph = laneGraphLoader.load(configuration.getGraphImpl());
//        if(graph.nodeCount() == 0){
//            graph.release();
//            return;
//        }

        final AnalysisAlgo analysisAlgo;
        analysisAlgo = new AnalysisAlgo(companyId, fileName, configuration, api, log)
                    .withProgressLogger(ProgressLogger.wrap(log, "AnalysisAlgo"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute();
    }
}
