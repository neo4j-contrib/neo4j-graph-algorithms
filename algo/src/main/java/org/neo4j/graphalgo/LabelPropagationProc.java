package org.neo4j.graphalgo;

import algo.Pools;
import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphalgo.impl.LabelPropagationExporter;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static algo.util.Util.parseDirection;

public final class LabelPropagationProc {

    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Procedure(name = "algo.labelPropagation", mode = Mode.WRITE)
    @Description("CALL algo.labelPropagation(times,labels,partitionKey,type,direction,weightKey) - simple label propagation kernel")
    public void labelPropagation(
            @Name("times") long times,
            @Name("labels") List<String> labelNames,
            @Name("partitionKey") String partitionKey,
            @Name("type") String relationshipType,
            @Name("direction") String directionName,
            @Name("weightKey") String weightKey
    ) throws ExecutionException {

        String label = labelNames != null && !labelNames.isEmpty()
                ? labelNames.get(0)
                : null;
        Direction direction = parseDirection(directionName);

        HeavyGraph graph = (HeavyGraph) new GraphLoader(dbAPI)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationshipType)
                .withRelationshipWeightsFromProperty(weightKey, 1.0d)
                .withNodeWeightsFromProperty(weightKey, 1.0d)
                .withNodeProperty(partitionKey, 0.0d)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);

        IntDoubleMap labels = new LabelPropagation(graph)
                .compute(direction, times);

        new LabelPropagationExporter(dbAPI, graph, partitionKey).write(labels);

    }
}
