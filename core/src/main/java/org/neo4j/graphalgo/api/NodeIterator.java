package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public interface NodeIterator {

    // TODO jdoc
    void forEachNode(IntConsumer consumer);

    PrimitiveIntIterator nodeIterator();
}
