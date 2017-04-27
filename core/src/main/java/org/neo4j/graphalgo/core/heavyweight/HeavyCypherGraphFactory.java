package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class HeavyCypherGraphFactory extends GraphFactory {

    private static final int BATCH_SIZE = 100_000;
    private static final int INITIAL_NODE_COUNT = 1_000_000;
    private static final int DEGREE = 10;

    public HeavyCypherGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api,setup);
    }

    @Override
    public Graph build() {
        return build(BATCH_SIZE);
    }

    @SuppressWarnings("WeakerAccess")
    Graph build(int batchSize) {
        final IdMap idMap = new IdMap(INITIAL_NODE_COUNT);
        boolean hasRelationshipWeights = !setup.loadDefaultRelationshipWeight();

        boolean hasNodeWeights = !setup.loadDefaultNodeWeight();
        final WeightMapping nodeWeights = hasNodeWeights ?
                new WeightMap(INITIAL_NODE_COUNT, setup.nodeDefaultWeight) :
                new NullWeightMap(setup.nodeDefaultWeight);

        boolean hasNodeProperty = !setup.loadDefaultNodeProperty();
        final WeightMapping nodeProps = hasNodeProperty ?
                new WeightMap(INITIAL_NODE_COUNT, setup.nodeDefaultPropertyValue) :
                new NullWeightMap(setup.nodeDefaultPropertyValue);

        // todo compiled
        api.execute(setup.nodeStatement).accept(row -> {
            long id = row.getNumber("id").longValue();
            idMap.add(id);
            if (hasNodeWeights) nodeWeights.set(id, row.get("weight"));
            if (hasNodeProperty) nodeProps.set(id, row.get("value"));
            return true;
        });

        int nodeCount = idMap.size();

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount);

        final WeightMapping relWeigths = hasRelationshipWeights ?
                new WeightMap(nodeCount * DEGREE, setup.relationDefaultWeight) :
                new NullWeightMap(setup.relationDefaultWeight);

        api.execute(setup.relationshipStatement).accept(new Result.ResultVisitor<RuntimeException>() {
            long lastSourceId = -1, lastTargetId = -1;
            int source = -1, target = -1;

            @Override
            public boolean visit(Result.ResultRow row) throws RuntimeException {
                long sourceId = row.getNumber("source").longValue();
                if (sourceId != lastSourceId) {
                    source = idMap.get(sourceId);
                    lastSourceId = sourceId;
                }
                if (source == -1) {
                    return true;
                }
                long targetId = row.getNumber("target").longValue();
                if (targetId != lastTargetId) {
                    target = idMap.get(targetId);
                    lastTargetId = targetId;
                }
                if (target == -1) {
                    return true;
                }
                long relId = RawValues.combineIntInt(source, target);
                if (hasRelationshipWeights) relWeigths.set(relId, row.get("weight"));
                // todo do we need relId for the matrix
                matrix.addOutgoing(source, target, relId);
                return true;
            }
        });

        return new HeavyGraph(idMap, matrix, relWeigths, nodeWeights, nodeProps);
    }
}
