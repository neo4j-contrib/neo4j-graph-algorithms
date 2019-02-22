package org.neo4j.graphalgo.similarity;

public interface SimilarityComputer<T> {
    SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff);
}
