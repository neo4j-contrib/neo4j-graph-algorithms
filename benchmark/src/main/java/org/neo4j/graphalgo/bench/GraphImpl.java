package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;

public enum GraphImpl {
    LIGHT(LightGraphFactory.class),
    HEAVY(HeavyGraphFactory.class),
    VIEW(GraphViewFactory.class);

    final Class<? extends GraphFactory> impl;

    GraphImpl(final Class<? extends GraphFactory> impl) {
        this.impl = impl;
    }
}
