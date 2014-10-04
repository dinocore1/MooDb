/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package com.sleepycat.je.tree;

import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
 * Caches the VLSN sequence for the LN entries in a BIN, when VLSN preservation
 * and caching are configured.
 *
 * A VLSN is added to the cache when an LN is evicted from a BIN.  When the LN
 * is resident, there is no need for caching because the LN contains the VLSN.
 * See BIN.setTarget.  This strategy works because an LN is always cached
 * during a read or write operation, and only evicted after that based on
 * eviction policies.
 */
public abstract class VLSNCache {

    public abstract long get(int idx);
    public abstract VLSNCache set(int idx, long val, IN parent);
    public abstract VLSNCache copy(int from, int to, int n);
    public abstract long getMemorySize();

    /**
     * An EMPTY_CACHE is used initially for each BIN until the need arises to
     * add to the cache.  The cache will remain empty if LNs are never evicted
     * or version caching is not configured, which is always the case for
     * standalone JE.
     */
    public static VLSNCache EMPTY_CACHE = new VLSNCache() {

        /**
         * The EMPTY_CACHE always return a null VLSN sequence.
         */
        @Override
        public long get(int idx) {
            return VLSN.NULL_VLSN.getSequence();
        }

        /**
         * When adding to the cache the EMPTY_CACHE is mutated into a
         * DefaultCache.
         */
        @Override
        public VLSNCache set(int idx, long val, IN parent) {

            if (!parent.getDatabase().getDbEnvironment().getCacheVLSN()) {
                return this;
            }

            if (val == VLSN.NULL_VLSN.getSequence()) {
                return this;
            }

            final VLSNCache newCache = new DefaultCache
                (parent.getMaxEntries(),
                 parent.getDatabase().
                        getDbEnvironment().
                        getCachedVLSNMinLength());

            parent.updateMemorySize(getMemorySize(), newCache.getMemorySize());

            return newCache.set(idx, val, parent);
        }

        @Override
        public VLSNCache copy(int from, int to, int n) {
            return this;
        }

        /**
         * An EMPTY_CACHE has no JE cache memory overhead because there is only
         * one global instance.
         */
        @Override
        public long getMemorySize() {
            return 0;
        }
    };

    public static class DefaultCache extends VLSNCache {

        /** Maximum value of a VLSN indexed by number of bytes. */
        private static long[] MAX_VLSN = {
            0x0L,
            0xFFL,
            0xFFFFL,
            0xFFFFFFL,
            0xFFFFFFFFL,
            0xFFFFFFFFFFL,
            0xFFFFFFFFFFFFL,
            0xFFFFFFFFFFFFFFL,
            0x7FFFFFFFFFFFFFFFL,
        };

        private final byte[] byteArray;
        private final int bytesPerVlsn;

        public DefaultCache(int capacity, int nBytes) {
            assert capacity >= 1;
            assert nBytes >= 1;
            assert nBytes <= 8;

            bytesPerVlsn = nBytes;
            byteArray = new byte[capacity * bytesPerVlsn];
        }

        /* Only for use by the Sizeof utility. */
        public DefaultCache(@SuppressWarnings("unused") SizeofMarker marker) {
            bytesPerVlsn = 0;
            byteArray = null;
        }

        @Override
        public long get(int idx) {

            int i = idx * bytesPerVlsn;
            final int end = i + bytesPerVlsn;

            long val = (byteArray[i] & 0xFF);

            for (i += 1; i < end; i += 1) {
                val <<= 8;
                val |= (byteArray[i] & 0xFF);
            }

            if (val == 0) {
                return VLSN.NULL_VLSN.getSequence();
            }

            return val;
        }

        /**
         * Mutates to a DefaultCache with a larger number of bytes if necessary
         * to hold the given value.
         */
        @Override
        public DefaultCache set(int idx, long val, IN parent) {

            assert idx >= 0;
            assert idx < byteArray.length / bytesPerVlsn;
            assert val != 0;

            if (val == VLSN.NULL_VLSN.getSequence()) {
                val = 0;
            }

            /*
             * If the value can't be represented using bytesPerVlsn, mutate
             * to a cache with a larger number of bytes.
             */
            if (val > MAX_VLSN[bytesPerVlsn]) {

                final int capacity = byteArray.length / bytesPerVlsn;

                DefaultCache newCache =
                    new DefaultCache(capacity, bytesPerVlsn + 1);

                parent.updateMemorySize(getMemorySize(),
                                        newCache.getMemorySize());

                /*
                 * Set new value in new cache, and copy other values from old
                 * cache.
                 */
                newCache = newCache.set(idx, val, parent);

                for (int i = 0; i < capacity; i += 1) {
                    if (i != idx) {
                        newCache = newCache.set(i, get(idx), parent);
                    }
                }

                return newCache;
            }

            /* Set VLSN sequence in this cache. */
            int i = ((idx + 1) * bytesPerVlsn) - 1;
            final int end = i - bytesPerVlsn;

            byteArray[i] = (byte) (val & 0xFF);

            for (i -= 1; i > end; i -= 1) {
                val >>= 8;
                byteArray[i] = (byte) (val & 0xFF);
            }

            assert ((val & 0xFFFFFFFFFFFFFF00L) == 0) : val;

            return this;
        }

        @Override
        public DefaultCache copy(int from, int to, int n) {
            System.arraycopy(byteArray,
                             from * bytesPerVlsn,
                             byteArray,
                             to * bytesPerVlsn,
                             n * bytesPerVlsn);
            return this;
        }

        @Override
        public long getMemorySize() {
            return MemoryBudget.DEFAULT_VLSN_CACHE_OVERHEAD +
                   MemoryBudget.byteArraySize(byteArray.length);
        }
    }
}
