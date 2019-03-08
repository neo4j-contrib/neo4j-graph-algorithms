package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */

public abstract class AbstractWriteBuilder<R> extends AbstractResultBuilder<R> {
    public abstract AbstractWriteBuilder<R> withWrite(boolean write);

    public abstract AbstractWriteBuilder<R>withProperty(String writeProperty) ;
}
