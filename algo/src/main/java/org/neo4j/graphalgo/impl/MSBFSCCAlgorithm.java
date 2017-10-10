package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.write.Exporter;

import java.util.function.LongToIntFunction;
import java.util.stream.Stream;

public abstract class MSBFSCCAlgorithm<ME extends MSBFSCCAlgorithm<ME>> extends Algorithm<ME> {

    public abstract Stream<MSClosenessCentrality.Result> resultStream();

    public abstract ME compute();

    public abstract LongToIntFunction farness();

    public abstract void export(String propertyName, Exporter exporter);

    public final double[] exportToArray() {
        return resultStream()
                .limit(Integer.MAX_VALUE)
                .mapToDouble(r -> r.centrality)
                .toArray();
    }

    static double centrality(int f, double k) {
        return f > 0 ? k / (double) f : 0D;
    }
}
