package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;

public final class HugeDisjointSetStructTranslator implements PropertyTranslator.OfLong<HugeDisjointSetStruct> {

    public static final PropertyTranslator<HugeDisjointSetStruct> INSTANCE = new HugeDisjointSetStructTranslator();

    @Override
    public long toLong(final HugeDisjointSetStruct data, final long nodeId) {
        return data.findNoOpt(nodeId);
    }
}
