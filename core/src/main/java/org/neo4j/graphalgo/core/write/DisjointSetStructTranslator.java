package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

public final class DisjointSetStructTranslator implements PropertyTranslator.OfInt<DisjointSetStruct> {

    public static final PropertyTranslator<DisjointSetStruct> INSTANCE = new DisjointSetStructTranslator();

    @Override
    public int toInt(final DisjointSetStruct data, final long nodeId) {
        return data.findNoOpt((int) nodeId);
    }
}
