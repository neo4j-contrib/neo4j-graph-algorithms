package org.neo4j.graphalgo.similarity;

interface SimilarityConsumer {
    void accept(int sourceIndex, int targetIndex, SimilarityResult result);
}
