package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.WeightMappingSerialization;
import org.neo4j.graphalgo.serialize.ByteBufferDataOutput;
import org.neo4j.graphalgo.serialize.IdMapSerialization;
import org.neo4j.graphalgo.serialize.PrivateLookup;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author phorn@avantgarde-labs.de
 */
public final class LightGraphFileWriter {

    private static final long BYTES_INT = Integer.BYTES;
    private static final long BYTES_LONG = Long.BYTES;

    private static final MethodHandle ID_MAP = PrivateLookup.field(
            LightGraph.class,
            IdMap.class,
            "idMapping");
    private static final MethodHandle WEIGHTS = PrivateLookup.field(
            LightGraph.class,
            WeightMap.class,
            "weightMapping");
    private static final MethodHandle ADJACENCY = PrivateLookup.field(
            LightGraph.class,
            IntArray.class,
            "adjacency");
    private static final MethodHandle IN_OFFSETS = PrivateLookup.field(
            LightGraph.class,
            long[].class,
            "inOffsets");
    private static final MethodHandle OUT_OFFSETS = PrivateLookup.field(
            LightGraph.class,
            long[].class,
            "outOffsets");

    public static void serialize(LightGraph graph, Path outFile) {

        try {
            IdMap idMapping = (IdMap) ID_MAP.invokeExact(graph);
            WeightMap weightMapping = (WeightMap) WEIGHTS.invokeExact(graph);
            IntArray adjacency = (IntArray) ADJACENCY.invokeExact(graph);
            long[] inOffsets = (long[]) IN_OFFSETS.invokeExact(graph);
            long[] outOffsets = (long[]) OUT_OFFSETS.invokeExact(graph);

            write(
                    outFile,
                    adjacency,
                    inOffsets,
                    outOffsets,
                    idMapping,
                    weightMapping
            );
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private static void write(
            Path outPath,
            IntArray adjacency,
            long[] inOffsets,
            long[] outOffsets,
            IdMap idMap,
            WeightMap weights) throws IOException {

        final long adjSize = adjacency.size();
        final long requiredBytes = BYTES_LONG
                + adjSize * BYTES_INT
                + BYTES_INT
                + inOffsets.length * BYTES_LONG
                + BYTES_INT
                + outOffsets.length * BYTES_LONG
                + IdMapSerialization.bytes(idMap)
                + WeightMappingSerialization.bytes(weights);

        final File file = outPath.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final FileChannel channel = raf.getChannel();
            channel.truncate(0);
            final MappedByteBuffer mbb = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    requiredBytes);
            final ByteBufferDataOutput out = new ByteBufferDataOutput(mbb);

            out.writeVLong(adjSize);
            for (IntArray.Cursor cursor : adjacency) {
                cursor.forEach(i -> {
                    out.writeVInt(i);
                    return true;
                });
            }

            out.writeVInt(inOffsets.length);
            for (long i : inOffsets) {
                out.writeVLong(i);
            }
            out.writeVInt(outOffsets.length);
            for (long i : outOffsets) {
                out.writeVLong(i);
            }

            IdMapSerialization.write(idMap, out);
            WeightMappingSerialization.write(weights, out);

            channel.truncate(out.written());
        }
    }
}
