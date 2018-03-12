package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public class Pointer {

    public static class BoolPointer {
        public boolean v;

        public BoolPointer(boolean v) {
            this.v = v;
        }
    }

    public static class IntPointer {
        public int v;

        public IntPointer(int v) {
            this.v = v;
        }
    }

    public static class LongPointer {
        public long v;

        public LongPointer(long v) {
            this.v = v;
        }
    }

    public static class DoublePointer {
        public double v;

        public DoublePointer(double v) {
            this.v = v;
        }
    }

    public static class GenericPointer<G>  {
        public G v;

        public GenericPointer(G v) {
            this.v = v;
        }
    }

    public static BoolPointer wrap(boolean v) {
        return new BoolPointer(v);
    }

    public static IntPointer wrap(int v) {
        return new IntPointer(v);
    }

    public static LongPointer wrap(long v) {
        return new LongPointer(v);
    }

    public static DoublePointer wrap(double v) {
        return new DoublePointer(v);
    }

    public static <T> GenericPointer wrap(T v) {
        return new GenericPointer<>(v);
    }
}
