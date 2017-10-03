package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.exporter.DoubleArrayExporter;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphalgo.results.DeltaSteppingProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 *
 * @author mknblch
 */
public class ShortestPathDeltaSteppingProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.shortestPath.deltaStepping.stream")
    @Description("CALL algo.shortestPath.deltaStepping.stream(startNode:Node, weightProperty:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0, concurrency:4}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, distance} from start to end (inclusive)")
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> deltaSteppingStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withLog(log)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getPropertyDefaultValue(Double.MAX_VALUE))
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        final ShortestPathDeltaStepping algo = new ShortestPathDeltaStepping(graph, delta)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths(DeltaStepping)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withExecutorService(Executors.newFixedThreadPool(
                        configuration.getConcurrency()
                )).compute(startNode.getId());

        graph.release();
        return algo.resultStream();
    }

    @Procedure(value = "algo.shortestPath.deltaStepping", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath.deltaStepping(startNode:Node, weightProperty:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0, write:true, writeProperty:'sssp'}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount")
    public Stream<DeltaSteppingProcResult> deltaStepping(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        final DeltaSteppingProcResult.Builder builder = DeltaSteppingProcResult.builder();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            graph = new GraphLoader(api, Pools.DEFAULT)
                    .withLog(log)
                    .withOptionalLabel(configuration.getNodeLabelOrQuery())
                    .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                    .withRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getPropertyDefaultValue(Double.MAX_VALUE))
                    .withDirection(Direction.OUTGOING)
                    .load(configuration.getGraphImpl());
        }

        final ShortestPathDeltaStepping algorithm = new ShortestPathDeltaStepping(graph, delta)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths(DeltaStepping)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withExecutorService(Pools.DEFAULT);

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            final double[] shortestPaths = algorithm.getShortestPaths();
            algorithm.release();
            graph.release();
            builder.timeWrite(() -> {
                new DoubleArrayExporter(api, graph, log,
                        configuration.get(WRITE_PROPERTY, DEFAULT_TARGET_PROPERTY), Pools.DEFAULT)
                        .withConcurrency(configuration.getConcurrency())
                        .write(shortestPaths);
            });
        }

        return Stream.of(builder
                .withNodeCount(graph.nodeCount())
                .build());
    }
}
