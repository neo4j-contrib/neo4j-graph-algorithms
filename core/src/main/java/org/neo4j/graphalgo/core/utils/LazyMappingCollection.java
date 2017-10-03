package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.AbstractIterator;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public final class LazyMappingCollection<T, U> extends AbstractCollection<U> {

    private final Collection<T> base;
    private final Function<T, U> mappingFunction;

    public static <T, U> Collection<U> of(
            final Collection<T> base,
            final Function<T, U> mappingFunction) {
        return new LazyMappingCollection<>(base, mappingFunction);
    }

    private LazyMappingCollection(
            final Collection<T> base,
            final Function<T, U> mappingFunction) {
        this.base = base;
        this.mappingFunction = mappingFunction;
    }

    @Override
    public Iterator<U> iterator() {
        return new AbstractIterator<U>() {
            private final Iterator<T> it = base.iterator();

            @Override
            protected U fetch() {
                if (it.hasNext()) {
                    return mappingFunction.apply(it.next());
                }
                return done();
            }
        };
    }

    @Override
    public int size() {
        return base.size();
    }
}
