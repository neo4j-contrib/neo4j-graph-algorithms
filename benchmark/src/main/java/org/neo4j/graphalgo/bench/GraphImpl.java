package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;

public enum GraphImpl {
    LIGHT(LightGraphFactory.class),
    HEAVY(HeavyGraphFactory.class),
    VIEW(GraphViewFactory.class),
    HUGE(HugeGraphFactory.class);

    final Class<? extends GraphFactory> impl;

    GraphImpl(Class<? extends GraphFactory> impl) {
        this.impl = impl;
    }
}
