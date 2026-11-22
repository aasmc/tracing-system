package ru.aasmc.tracing.collector.domain.resource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** The xxHash64 algorithm with seed 0, as specified at https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md. */
final class XxHash64 {

    private static final long PRIME_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME_3 = 0x165667B19E3779F9L;
    private static final long PRIME_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME_5 = 0x27D4EB2F165667C5L;
    private static final int STRIPE_BYTES = 32;

    private XxHash64() {}

    static long hash(byte[] input) {
        ByteBuffer bytes = ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN);
        int length = input.length;
        int offset = 0;
        long hash;
        if (length >= STRIPE_BYTES) {
            long acc1 = PRIME_1 + PRIME_2;
            long acc2 = PRIME_2;
            long acc3 = 0;
            long acc4 = -PRIME_1;
            while (offset <= length - STRIPE_BYTES) {
                acc1 = round(acc1, bytes.getLong(offset));
                acc2 = round(acc2, bytes.getLong(offset + 8));
                acc3 = round(acc3, bytes.getLong(offset + 16));
                acc4 = round(acc4, bytes.getLong(offset + 24));
                offset += STRIPE_BYTES;
            }
            hash = Long.rotateLeft(acc1, 1)
                    + Long.rotateLeft(acc2, 7)
                    + Long.rotateLeft(acc3, 12)
                    + Long.rotateLeft(acc4, 18);
            hash = mergeAccumulator(hash, acc1);
            hash = mergeAccumulator(hash, acc2);
            hash = mergeAccumulator(hash, acc3);
            hash = mergeAccumulator(hash, acc4);
        } else {
            hash = PRIME_5;
        }
        hash += length;
        for (; offset + 8 <= length; offset += 8) {
            hash ^= round(0, bytes.getLong(offset));
            hash = Long.rotateLeft(hash, 27) * PRIME_1 + PRIME_4;
        }
        if (offset + 4 <= length) {
            hash ^= Integer.toUnsignedLong(bytes.getInt(offset)) * PRIME_1;
            hash = Long.rotateLeft(hash, 23) * PRIME_2 + PRIME_3;
            offset += 4;
        }
        for (; offset < length; offset++) {
            hash ^= Byte.toUnsignedLong(input[offset]) * PRIME_5;
            hash = Long.rotateLeft(hash, 11) * PRIME_1;
        }
        return avalanche(hash);
    }

    private static long round(long accumulator, long lane) {
        return Long.rotateLeft(accumulator + lane * PRIME_2, 31) * PRIME_1;
    }

    private static long mergeAccumulator(long hash, long accumulator) {
        return (hash ^ round(0, accumulator)) * PRIME_1 + PRIME_4;
    }

    private static long avalanche(long hash) {
        hash ^= hash >>> 33;
        hash *= PRIME_2;
        hash ^= hash >>> 29;
        hash *= PRIME_3;
        return hash ^ (hash >>> 32);
    }
}
