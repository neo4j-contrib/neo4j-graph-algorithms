package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;

public abstract class GraphUnionFindAlgo<G extends Graph, R, ME extends GraphUnionFindAlgo<G, R, ME>> extends Algorithm<ME> {

    protected G graph;

    GraphUnionFindAlgo(final G graph) {
        this.graph = graph;
    }

    public abstract R compute();

    public abstract R compute(double threshold);

    @Override
    public ME me() {
        //noinspection unchecked
        return (ME) this;
    }

    @Override
    public ME release() {
        graph = null;
        return me();
    }
}
