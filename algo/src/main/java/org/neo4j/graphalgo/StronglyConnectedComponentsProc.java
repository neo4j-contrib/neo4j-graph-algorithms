package org.neo4j.graphalgo;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.SCCTarjan;
import org.neo4j.graphalgo.impl.SCCTarjanExporter;
import org.neo4j.graphalgo.results.SCCResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class StronglyConnectedComponentsProc {

    public static final String CONFIG_WRITE = "write";
    public static final String CONFIG_WRITE_PROPERTY = "clusterProperty";
    public static final String CONFIG_CLUSTER = "cluster";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.scc", mode = Mode.WRITE)
    @Description("CALL algo.scc(label:String, relationship:String, config:Map<String, Object>) YIELDS " +
            "loadDuration, evalDuration, writeDuration, setCount, maxSetSize, minSetSize")
    public Stream<SCCResult> sccTarjan(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        SCCResult.Builder builder = SCCResult.builder();

        ProgressTimer loadTimer = builder.timeLoad();
        Graph graph = new GraphLoader(api)
                    .withOptionalLabel(label)
                    .withOptionalRelationshipType(relationship)
                    .withoutRelationshipWeights()
                    .withExecutorService(Pools.DEFAULT)
                    .load(HeavyGraphFactory.class);
        loadTimer.stop();

        SCCTarjan tarjan = new SCCTarjan(graph);
        builder.timeEval(() -> {
            tarjan.compute();
            builder.withMaxSetSize(tarjan.getMaxSetSize())
                    .withMinSetSize(tarjan.getMinSetSize())
                    .withSetCount(tarjan.getConnectedComponents().size());
        });

        if ((boolean) config.getOrDefault(CONFIG_WRITE, Boolean.FALSE)) {
            builder.timeWrite(() -> {
                new SCCTarjanExporter(api)
                        .withIdMapping(graph)
                        .withWriteProperty((String) config.getOrDefault(CONFIG_WRITE_PROPERTY, CONFIG_CLUSTER))
                        .write(tarjan.getConnectedComponents());
            });
        }

        return Stream.of(builder.build());
    }
}
