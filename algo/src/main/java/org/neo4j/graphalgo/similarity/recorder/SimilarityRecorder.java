package org.neo4j.graphalgo.similarity.recorder;

import org.neo4j.graphalgo.similarity.Computations;
import org.neo4j.graphalgo.similarity.SimilarityComputer;
import org.neo4j.graphalgo.similarity.SimilarityProc;

public interface SimilarityRecorder<T> extends Computations, SimilarityComputer<T> {
}
