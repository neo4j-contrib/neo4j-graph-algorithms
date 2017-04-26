package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphViewFactory extends GraphFactory {

    public GraphViewFactory(
            final GraphDatabaseAPI api,
            final GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        return new GraphView(
                api,
                setup.startLabel,
                setup.relationshipType,
                setup.relationWeightPropertyName,
                setup.relationDefaultWeight);
    }
}
