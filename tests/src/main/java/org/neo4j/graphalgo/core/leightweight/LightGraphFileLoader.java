package org.neo4j.graphalgo.core.leightweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.serialize.ByteBufferDataInput;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMappingSerialization;
import org.neo4j.graphalgo.serialize.IdMapSerialization;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author phorn@avantgarde-labs.de
 */
public class LightGraphFileLoader {

    public static Graph load(Path inFile) throws IOException {
        final File file = inFile.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final FileChannel channel = raf.getChannel();
            final MappedByteBuffer mbb = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size());
            final ByteBufferDataInput in = new ByteBufferDataInput(mbb);

            final long adjacencyLength = in.readVLong();
            final IntArray adjacency = IntArray.newArray(adjacencyLength);
            adjacency.fill(0, adjacencyLength, () -> {
                try {
                    return in.readVInt();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            final int inOffsetsLength = in.readVInt();
            final long[] inOffsets = new long[inOffsetsLength];
            for (int i = 0; i < inOffsetsLength; i++) {
                inOffsets[i] = in.readVLong();
            }

            final int outOffsetsLength = in.readVInt();
            final long[] outOffsets = new long[outOffsetsLength];
            for (int i = 0; i < outOffsetsLength; i++) {
                outOffsets[i] = in.readVLong();
            }

            final IdMap idMap = IdMapSerialization.read(in);
            final WeightMapping weightMapping = WeightMappingSerialization.read(in);

            return new LightGraph(
                    idMap, weightMapping,
                    adjacency, inOffsets, outOffsets
            );
        }
    }
}
