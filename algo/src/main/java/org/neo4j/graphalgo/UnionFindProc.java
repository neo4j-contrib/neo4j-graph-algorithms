package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.results.UnionFindResult;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class UnionFindProc {

    public static final String CONFIG_PROPERTY = "property";
    public static final String CONFIG_DEFAULT_VALUE = "defaultValue";
    public static final String CONFIG_THRESHOLD = "threshold";
    public static final String CONFIG_CLUSTER_PROPERTY = "clusterProperty";
    public static final String DEFAULT_CLUSTER_PROPERTY = "cluster";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.unionFind", mode = Mode.WRITE)
    @Description("CALL algo.unionFind(label:String, relationship:String, " +
            "{property:'propertyName', threshold:0.42, defaultValue:1.0, write: true, clusterProperty:'cluster'}) " +
            "YIELD setCount, loadDuration, evalDuration, writeDuration")
    public Stream<UnionFindResult> unionFind(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        UnionFindResult.Builder resultBuilder = UnionFindResult.builder();

        // loading
        ProgressTimer load = ProgressTimer.start(resultBuilder::withLoadDuration);
        final Graph graph = load(label, relationship, config);
        load.stop();

        // evaluation
        ProgressTimer eval = ProgressTimer.start(resultBuilder::withEvalDuration);
        final DisjointSetStruct struct = evaluate(graph, config);
        eval.stop();

        if ((Boolean) config.getOrDefault("write", Boolean.FALSE)) {
            // write back
            ProgressTimer writeTimer = ProgressTimer.start(resultBuilder::withWriteDuration);
            write(graph, struct, config);
            writeTimer.stop();
        }

        return Stream.of(resultBuilder
                .withNodeCount(graph.nodeCount())
                .withSetCount(struct.getSetCount())
                .build());
    }

    @Procedure(value = "algo.unionFindStream")
    @Description("CALL algo.unionFindStream(label:String, relationship:String, " +
            "{property:'propertyName', threshold:0.42, defaultValue:1.0) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFindStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        // loading
        final Graph graph = load(label, relationship, config);

        // evaluation
        return evaluate(graph, config)
                .resultStream(graph);
    }

    private Graph load(String label, String relationship, Map<String, Object> config) {
        return new GraphLoader(api)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withOptionalWeightsFromProperty(
                        (String) config.get(CONFIG_PROPERTY),
                        (double)config.getOrDefault(CONFIG_DEFAULT_VALUE, 1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
    }

    private DisjointSetStruct evaluate(Graph graph, Map<String, Object> config) {
        final DisjointSetStruct struct;
        if (config.containsKey(CONFIG_PROPERTY) && config.containsKey(CONFIG_THRESHOLD)) {
            log.debug("Computing union find with threshold " + config.get(CONFIG_THRESHOLD));
            struct = new GraphUnionFind(graph)
                    .compute((double) config.getOrDefault(CONFIG_THRESHOLD, 0.0));
        } else {
            log.debug("Computing union find without threshold");
            struct = new GraphUnionFind(graph)
                    .compute();
        }
        return struct;
    }

    private void write(Graph graph, DisjointSetStruct struct, Map<String, Object> config) {
        log.debug("Writing results");
        new DisjointSetStruct.DSSExporter(api,
                graph,
                (String) config.getOrDefault(CONFIG_CLUSTER_PROPERTY, DEFAULT_CLUSTER_PROPERTY))
                .write(struct);
    }

}
