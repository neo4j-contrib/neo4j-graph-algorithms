package org.neo4j.graphalgo;

import algo.Pools;
import com.carrotsearch.hppc.IntIntMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mknblch
 */
public class UnionFindProc {

    public static final String CONFIG_PROPERTY = "property";
    public static final String CONFIG_DEFAULT_VALUE = "defaultValue";
    public static final String CONFIG_THRESHOLD = "threshold";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Procedure(value = "algo.unionFind")
    @Description("CALL algo.unionFind(label:String, relationship:String, {property:'propertyName', threshold:0.42, defaultValue:1.0}) " +
            "YIELD nodeId, setId - yields a setId to each node id")
    public Stream<DisjointSetStruct.Result> unionFind(
            @Name("label") String label,
            @Name("relationship") String relationship,
            @Name("config") Map<String, Object> properties) {

        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withRelationshipType(relationship)
                .withOptionalWeightsFromProperty(
                        (String) properties.get(CONFIG_PROPERTY),
                        (double)properties.getOrDefault(CONFIG_DEFAULT_VALUE, 1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        if (properties.containsKey(CONFIG_PROPERTY) && properties.containsKey(CONFIG_THRESHOLD)) {
            log.debug("Computing union find with threshold " + properties.get(CONFIG_THRESHOLD));
            return new GraphUnionFind(graph)
                    .compute((double)properties.getOrDefault(CONFIG_THRESHOLD, 0.0))
                    .resultStream(graph);
        } else {
            log.debug("Computing union find without threshold");
            return new GraphUnionFind(graph)
                    .compute()
                    .resultStream(graph);
        }
    }

    @Procedure(value = "algo.unionFindStats")
    @Description("CALL algo.unionFindStats(label:String, relationship:String, {property:'propertyName', threshold:0.42, defaultValue:1.0}) " +
            "YIELD setId, setSize - yields the size for each setId")
    public Stream<Result> unionFindStats(
            @Name("label") String label,
            @Name("relationship") String relationship,
            @Name("config") Map<String, Object> properties) {

        Graph graph = new GraphLoader(api)
                .withOptionalLabel(label)
                .withRelationshipType(relationship)
                .withOptionalWeightsFromProperty(
                        (String) properties.get(CONFIG_PROPERTY),
                        (double)properties.getOrDefault(CONFIG_DEFAULT_VALUE, 1.0))
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        final IntIntMap setSize;
        if (properties.containsKey(CONFIG_PROPERTY) && properties.containsKey(CONFIG_THRESHOLD)) {
            log.debug("Computing threshold union find");
            setSize = new GraphUnionFind(graph)
                    .compute((double) properties.getOrDefault(CONFIG_THRESHOLD, 0.0))
                    .getSetSize();
        } else {
            log.debug("Computing union find without threshold");
            setSize = new GraphUnionFind(graph)
                    .compute((double) properties.getOrDefault(CONFIG_THRESHOLD, 0.0))
                    .getSetSize();
        }

        return StreamSupport.stream(setSize.spliterator(), false)
                .map(cursor -> new Result(cursor.key, cursor.value));
    }


    public static class Result {

        public final Long setId;
        public final Long setSize;

        public Result(long setId, long setSize) {
            this.setId = setId;
            this.setSize = setSize;
        }
    }
}
