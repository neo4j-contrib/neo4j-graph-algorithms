package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;

public interface IdMap extends IdMapping, NodeIterator, BatchNodeIterable {
}
