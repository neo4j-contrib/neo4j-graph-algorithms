package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class LightGraphFactory extends GraphFactory {

    public LightGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        try {
            return importGraph();
        } catch (EntityNotFoundException e) {
            throw Exceptions.launderedException(e);
        }
    }

    private Graph importGraph() throws EntityNotFoundException {
        final IdMap idMap = loadIdMap();
        final GraphImporter graphImporter = new GraphImporter(
                api,
                setup,
                idMap,
                newWeightMap(dimensions.weightId(), setup.relationDefaultWeight),
                dimensions.nodeCount(),
                dimensions.relationId()
        );
        return graphImporter.call();
    }
}
