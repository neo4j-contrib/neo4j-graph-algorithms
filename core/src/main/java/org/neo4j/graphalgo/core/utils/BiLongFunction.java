package org.neo4j.graphalgo.core.utils;

@FunctionalInterface
public interface BiLongFunction<R> {

    R apply(long value1, long value2);
}
