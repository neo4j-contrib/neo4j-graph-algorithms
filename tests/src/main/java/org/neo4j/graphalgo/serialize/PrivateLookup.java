package org.neo4j.graphalgo.serialize;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Field;

public final class PrivateLookup {

    private static final Lookup LOOKUP;

    static {
        try {
            Field implLookup = Lookup.class.getDeclaredField("IMPL_LOOKUP");
            implLookup.setAccessible(true);
            LOOKUP = (Lookup) implLookup.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    public static <T, U> MethodHandle field(
            Class<T> parentClass,
            Class<U> fieldClass,
            String name) {
        try {
            return LOOKUP.findGetter(
                    parentClass,
                    name,
                    fieldClass);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    private PrivateLookup() {
    }
}
