package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphViewFactory extends GraphFactory {

    public GraphViewFactory(
            final GraphDatabaseAPI api,
            final GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph build() {
        final Direction direction;
        if (setup.loadOutgoing) {
            direction = setup.loadIncoming ? Direction.BOTH : Direction.OUTGOING;
        } else {
            direction = setup.loadIncoming ? Direction.INCOMING : null;
        }

        return new GraphView(
                api,
                direction,
                setup.startLabel,
                setup.relationshipType,
                setup.relationWeightPropertyName,
                setup.relationDefaultWeight);
    }
}
