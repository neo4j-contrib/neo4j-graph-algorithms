package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @see <a href="http://mechanical-sympathy.blogspot.ch/2011/08/false-sharing-java-7.html">http://mechanical-sympathy.blogspot.ch/2011/08/false-sharing-java-7.html</a>
 */
@SuppressWarnings({"serial", "WeakerAccess"})
public final class PaddedAtomicLong extends AtomicLong {
    public volatile long p1 = 1, p2 = 2, p3 = 3, p4 = 4, p5 = 5, p6 = 6, p7 = 7;

    public long sum() { // prevents optimizing away the fields above
        return p1 + p2 + p3 + p4 + p5 + p6 + p7;
    }

    public PaddedAtomicLong() {
        super();
    }

    public PaddedAtomicLong(long initialValue) {
        super(initialValue);
    }
}
