package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

public class LRUQueryCacheV2 implements QueryCache, Accountable, Closeable {

    private final int maxSize;
    private final long maxRamBytesUsed;
    private final Predicate<LeafReaderContext> leavesToCache;
    // maps queries that are contained in the cache to a singleton so that this
    // cache does not store several copies of the same query
    private final Map<Query, Query> uniqueQueries;
    // The contract between this set and the per-leaf caches is that per-leaf caches
    // are only allowed to store sub-sets of the queries that are contained in
    // mostRecentlyUsedQueries. This is why write operations are performed under a lock
    private final Set<Query> mostRecentlyUsedQueries;

    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final float skipCacheFactor;
    private final LongAdder hitCount;
    private final LongAdder missCount;

    // these variables are volatile so that we do not need to sync reads
    // but increments need to be performed under the lock
    private volatile LongAdder ramBytesUsed;
    private volatile LongAdder cacheCount;
    private volatile LongAdder cacheSize;

    private final ConcurrentMap<IndexReader.CacheKey, Boolean> registeredClosedListeners = new ConcurrentHashMap<>();

    private final Set<IndexReader.CacheKey> keysToClean = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ScheduledExecutorService executorService;

    private final int numberOfSegments;

    private final LRUQueryCacheSegment[] lruQueryCacheSegment;

    public Map<Integer, AtomicInteger> partitionNumberMap = new ConcurrentHashMap<>();



    /**
     * Expert: Create a new instance that will cache at most <code>maxSize</code> queries with at most
     * <code>maxRamBytesUsed</code> bytes of memory, only on leaves that satisfy {@code
     * leavesToCache}.
     *
     * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
     * top-level query will not be cached in order to not slow down queries too much.
     */
    public LRUQueryCacheV2(
        int maxSize,
        long maxRamBytesUsed,
        Predicate<LeafReaderContext> leavesToCache,
        float skipCacheFactor,
        int numberOfSegments) {
        this.maxSize = maxSize;
        this.maxRamBytesUsed = maxRamBytesUsed;
        this.leavesToCache = leavesToCache;
        if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
            throw new IllegalArgumentException(
                "skipCacheFactor must be no less than 1, get " + skipCacheFactor);
        }
        this.skipCacheFactor = skipCacheFactor;

        uniqueQueries = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
        mostRecentlyUsedQueries = uniqueQueries.keySet();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        writeLock = lock.writeLock();
        readLock = lock.readLock();
        ramBytesUsed = new LongAdder();
        cacheCount = new LongAdder();
        cacheSize = new LongAdder();
        hitCount = new LongAdder();
        missCount = new LongAdder();
        executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("query-cache-cleaner"));
        executorService.schedule(this::cleanUp, 1, TimeUnit.MINUTES);

        this.numberOfSegments = numberOfSegments;
        this.lruQueryCacheSegment = new LRUQueryCacheSegment[numberOfSegments];
        int maxSizePerSegment = maxSize / this.numberOfSegments;
        long maxSizeInBytesPerSegment = (maxRamBytesUsed / this.numberOfSegments);
        for (int i = 0; i < numberOfSegments; i++) {
            lruQueryCacheSegment[i] = new LRUQueryCacheSegment(maxSizePerSegment, maxSizeInBytesPerSegment);
        }
    }

    public LRUQueryCacheV2(int maxSize, long maxRamBytesUsed) {
        this(maxSize, maxRamBytesUsed, new LRUQueryCache.MinSegmentSizePredicate(10000), 10, 16);
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < numberOfSegments; i++) {
            lruQueryCacheSegment[i].close();
        }
        this.executorService.close();
    }

    // pkg-private for testing
    static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
        private final int minSize;

        MinSegmentSizePredicate(int minSize) {
            this.minSize = minSize;
        }

        @Override
        public boolean test(LeafReaderContext context) {
            final int maxDoc = context.reader().maxDoc();
            if (maxDoc < minSize) {
                return false;
            }
            final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
            final int averageTotalDocs =
                topLevelContext.reader().maxDoc() / topLevelContext.leaves().size();
            return maxDoc * 2 > averageTotalDocs;
        }
    }


    /**
     * Expert: callback when the cache is completely cleared.
     *
     * @lucene.experimental
     */
    protected void onClear() {
        assert writeLock.isHeldByCurrentThread();
        ramBytesUsed.reset();
        cacheSize.reset();
    }

    public class LRUQueryCacheSegment implements Closeable {
        private final ReentrantReadWriteLock.ReadLock readLock;
        private final ReentrantReadWriteLock.WriteLock writeLock;
        private final Set<Query> mostRecentlyUsedQueries;

        private final Map<Query, Query> uniqueQueries;

        private final int maxSize;
        private final long maxRamBytesUsed;
        private final Map<QueryCacheKey, LRUQueryCache.CacheAndCount> cache;

        LRUQueryCacheSegment(int maxSize, long maxRamBytesUsed) {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            writeLock = lock.writeLock();
            readLock = lock.readLock();
            uniqueQueries = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
            mostRecentlyUsedQueries = uniqueQueries.keySet();
            this.maxSize = maxSize;
            this.maxRamBytesUsed = maxRamBytesUsed;
            cache = new ConcurrentHashMap<>();
        }

        // pkg-private for testing
        // return the list of cached queries in LRU order
        List<Query> cachedQueries() {
            readLock.lock();
            try {
                return new ArrayList<>(mostRecentlyUsedQueries);
            } finally {
                readLock.unlock();
            }
        }

        public void clearQuery(Query query) {
            writeLock.lock();
            try {
                final Query singleton = uniqueQueries.remove(query);
                if (singleton != null) {
                    onEviction(singleton);
                }
            } finally {
                writeLock.unlock();
            }
        }

        LRUQueryCache.CacheAndCount get(QueryCacheKey queryCacheKey, Query key, IndexReader.CacheHelper cacheHelper) {
            readLock.lock();
            try {
                assert key instanceof BoostQuery == false;
                assert key instanceof ConstantScoreQuery == false;
                final IndexReader.CacheKey readerKey = cacheHelper.getKey();
                //final LRUQueryCacheV2.LeafCache leafCache = cache.get(readerKey);

                LRUQueryCache.CacheAndCount count = cache.get(queryCacheKey);
                final Query singleton = uniqueQueries.get(key);
                if (singleton == null) {
                    onMiss(readerKey, key);
                    return null;
                }
                if (count == null) {
                    onMiss(readerKey, key);
                } else {
                    onHit(readerKey, key);
                }
                return count;
            } finally {
                readLock.unlock();
            }
        }

        protected void onClear() {
            assert writeLock.isHeldByCurrentThread();
            ramBytesUsed.reset();
            cacheSize.reset();
        }

        void remove(Query query) {
        }

        public void clear() {
            writeLock.lock();
            try {
                cache.clear();
                // Note that this also clears the uniqueQueries map since mostRecentlyUsedQueries is the
                // uniqueQueries.keySet view:
                mostRecentlyUsedQueries.clear();
                onClear();
            } finally {
                writeLock.unlock();
            }
        }

        public void putIfAbsent(QueryCacheKey queryCacheKey, Query query,
                                IndexReader.CacheHelper cacheHelper, LRUQueryCache.CacheAndCount cached) {
            assert query instanceof BoostQuery == false;
            assert query instanceof ConstantScoreQuery == false;
            writeLock.lock();
            try {
                uniqueQueries.putIfAbsent(query, query);
                if (cache.putIfAbsent(queryCacheKey, cached) == null) {
                    ramBytesUsed.add(HASHTABLE_RAM_BYTES_PER_ENTRY);
                    onDocIdSetCache(cacheHelper.getKey(), cached.ramBytesUsed());
                }
                if (registeredClosedListeners.putIfAbsent(cacheHelper.getKey(), Boolean.TRUE) == null) {
                    // we just created a new leaf cache, need to register a close listener
                    cacheHelper.addClosedListener(LRUQueryCacheV2.this::clearCoreCacheKey);
                }
                evictIfNecessary();
            } finally {
                writeLock.unlock();
            }
        }

        boolean requiresEviction() {
            final int size = mostRecentlyUsedQueries.size();
            if (size == 0) {
                return false;
            } else {
                return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
            }
        }

        private void evictIfNecessary() {
            assert writeLock.isHeldByCurrentThread();
            // under a lock to make sure that mostRecentlyUsedQueries and cache keep sync'ed
            if (requiresEviction()) {

                Iterator<Query> iterator = mostRecentlyUsedQueries.iterator();
                do {
                    final Query query = iterator.next();
                    final int size = mostRecentlyUsedQueries.size();
                    iterator.remove();
                    if (size == mostRecentlyUsedQueries.size()) {
                        // size did not decrease, because the hash of the query changed since it has been
                        // put into the cache
                        throw new ConcurrentModificationException(
                            "Removal from the cache failed! This "
                                + "is probably due to a query which has been modified after having been put into "
                                + " the cache or a badly implemented clone(). Query class: ["
                                + query.getClass()
                                + "], query: ["
                                + query
                                + "]");
                    }
                    onEviction(query);
                } while (iterator.hasNext() && requiresEviction());
            }
        }

        public void cleanUp(Set<IndexReader.CacheKey> keysToCleanUpSnapshot) {
            for (Iterator<QueryCacheKey> iterator = cache.keySet().iterator(); iterator.hasNext();) {
                QueryCacheKey queryCacheKey = iterator.next();
                IndexReader.CacheKey cacheKey = queryCacheKey.cacheKey;
                if (keysToCleanUpSnapshot.contains(cacheKey)) {
                    iterator.remove();
                }
            }
        }

        @Override
        public void close() throws IOException {
            this.clear();
        }
    }

    private class QueryCacheKey implements Accountable {

        private final static long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(QueryCacheKey.class);

        IndexReader.CacheKey cacheKey;
        Query query;

        QueryCacheKey(IndexReader.CacheKey cacheKey, Query query) {
            this.cacheKey = cacheKey;
            this.query = query;
        }

        @Override
        public int hashCode() {
            int res = System.identityHashCode(cacheKey);
            res = 31 * res + System.identityHashCode(query);
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryCacheKey queryCacheKey = (QueryCacheKey) o;
            if (!Objects.equals(cacheKey, queryCacheKey.cacheKey)) return false;
            if (!Objects.equals(query, queryCacheKey.query)) return false;
            return true;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + getRamBytesUsed(query);
        }
    }

    private synchronized void cleanUp() {
        Set<IndexReader.CacheKey> keysToCleanUpSnapshot = ConcurrentHashMap.newKeySet();
        synchronized (keysToClean) {
            keysToCleanUpSnapshot.addAll(keysToClean);
            keysToClean.clear();
        }
        for (int i = 0; i < this.numberOfSegments; i++) {
            lruQueryCacheSegment[i].cleanUp(keysToCleanUpSnapshot);
        }
        keysToCleanUpSnapshot = null;
    }

    // pkg-private for testing
    // return the list of cached queries in LRU order
    List<Query> cachedQueries() {
        List<Query> list = new ArrayList<Query>();
        for (int i = 0; i < this.numberOfSegments; i++) {
            List<Query> segmentQueryList = lruQueryCacheSegment[i].cachedQueries();
            list.addAll(segmentQueryList);
        }
        return list;
    }


    /**
     * Expert: callback when there is a cache miss on a given query.
     *
     * @see #onHit
     * @lucene.experimental
     */
    protected void onMiss(Object readerCoreKey, Query query) {
        assert query != null;
        missCount.add(1);
    }

    @Override
    /** Clear the content of this cache. */
    public void clear() {
        for (int i = 0; i < this.numberOfSegments; i++) {
            this.lruQueryCacheSegment[i].clear();
        }
    }

    /** Remove all cache entries for the given query. */
    public void clearQuery(Query query) {
        for (int i = 0; i < this.numberOfSegments; i++) {
            // We don't know which segment does the query belongs.
            this.lruQueryCacheSegment[i].clearQuery(query);
        }
    }

    /**
     * Expert: callback when there is a cache hit on a given query. Implementing this method is
     * typically useful in order to compute more fine-grained statistics about the query cache.
     *
     * @see #onMiss
     * @lucene.experimental
     */
    protected void onHit(Object readerCoreKey, Query query) {
        hitCount.add(1);
    }

    @Override
    public LRUQueryCache.CacheAndCount get(Query key, IndexReader.CacheHelper cacheHelper) {
        assert key instanceof BoostQuery == false;
        assert key instanceof ConstantScoreQuery == false;
        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        //final LRUQueryCacheV2.LeafCache leafCache = cache.get(readerKey);

        QueryCacheKey queryCacheKey = new QueryCacheKey(readerKey, key);
        int partitionNumber = queryCacheKey.hashCode() & (this.numberOfSegments - 1);
        LRUQueryCache.CacheAndCount count = this.lruQueryCacheSegment[partitionNumber].get(queryCacheKey, key,
            cacheHelper);
        if (count == null) {
            onMiss(readerKey, key);
        } else {
            onHit(readerKey, key);
        }
        return count;
    }

    /**
     * Expert: callback when a {@link DocIdSet} is added to this cache. Implementing this method is
     * typically useful in order to compute more fine-grained statistics about the query cache.
     *
     * //@see #onDocIdSetEviction
     * @lucene.experimental
     */
    protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
        cacheSize.add(1);
        cacheCount.add(1);
        this.ramBytesUsed.add(ramBytesUsed);
    }

    /**
     * Expert: callback when one or more {@link DocIdSet}s are removed from this cache.
     *
     * @see #onDocIdSetCache
     * @lucene.experimental
     */
    protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        this.ramBytesUsed.add(-sumRamBytesUsed);
        cacheSize.add(-numEntries);
    }



    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof LRUQueryCache.CachingWrapperWeight) {
            weight = ((CachingWrapperWeightV2) weight).in;
        }

        return new CachingWrapperWeightV2(weight, policy);
    }




    private class CachingWrapperWeightV2 extends ConstantScoreWeight {

        private final Weight in;
        private final QueryCachingPolicy policy;
        // we use an AtomicBoolean because Weight.scorer may be called from multiple
        // threads when IndexSearcher is created with threads
        private final AtomicBoolean used;

        CachingWrapperWeightV2(Weight in, QueryCachingPolicy policy) {
            super(in.getQuery(), 1f);
            this.in = in;
            this.policy = policy;
            used = new AtomicBoolean(false);
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return in.matches(context, doc);
        }

        private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
            // The worst-case (dense) is a bit set which needs one bit per document
            final long worstCaseRamUsage = maxDoc / 8;
            final long totalRamAvailable = maxRamBytesUsed;
            // Imagine the worst-case that a cache entry is large than the size of
            // the cache: not only will this entry be trashed immediately but it
            // will also evict all current entries from the cache. For this reason
            // we only cache on an IndexReader if we have available room for
            // 5 different filters on this reader to avoid excessive trashing
            return worstCaseRamUsage * 5 < totalRamAvailable;
        }

        /** Check whether this segment is eligible for caching, regardless of the query. */
        private boolean shouldCache(LeafReaderContext context) throws IOException {
            return cacheEntryHasReasonableWorstCaseSize(
                ReaderUtil.getTopLevelContext(context).reader().maxDoc())
                && leavesToCache.test(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.scorerSupplier(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.scorerSupplier(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cache helper
                return in.scorerSupplier(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            if (readLock.tryLock() == false) {
                return in.scorerSupplier(context);
            }

            LRUQueryCache.CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }

            if (cached == null) {
                if (policy.shouldCache(in.getQuery())) {
                    final ScorerSupplier supplier = in.scorerSupplier(context);
                    if (supplier == null) {
                        putIfAbsent(in.getQuery(), LRUQueryCache.CacheAndCount.EMPTY, cacheHelper);
                        return null;
                    }

                    final long cost = supplier.cost();
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            // skip cache operation which would slow query down too much
                            if (cost / skipCacheFactor > leadCost) {
                                return supplier.get(leadCost);
                            }

                            LRUQueryCache.CacheAndCount cached = cacheImpl(supplier.bulkScorer(), context.reader().maxDoc());
                            putIfAbsent(in.getQuery(), cached, cacheHelper);
                            DocIdSetIterator disi = cached.iterator();
                            if (disi == null) {
                                // docIdSet.iterator() is allowed to return null when empty but we want a non-null
                                // iterator here
                                disi = DocIdSetIterator.empty();
                            }

                            return new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, disi);
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                } else {
                    return in.scorerSupplier(context);
                }
            }

            assert cached != null;
            if (cached == LRUQueryCache.CacheAndCount.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = cached.iterator();
            if (disi == null) {
                return null;
            }

            return new ScorerSupplier() {
                @Override
                public Scorer get(long LeadCost) throws IOException {
                    return new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, disi);
                }

                @Override
                public long cost() {
                    return disi.cost();
                }
            };
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            // Our cache won't have an accurate count if there are deletions
            if (context.reader().hasDeletions()) {
                return in.count(context);
            }

            // Otherwise check if the count is in the cache
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.count(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.count(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cacheHelper
                return in.count(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            if (readLock.tryLock() == false) {
                return in.count(context);
            }

            LRUQueryCache.CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }
            if (cached != null) {
                // cached
                return cached.count();
            }
            // Not cached, check if the wrapped weight can count quickly then use that
            return in.count(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return in.isCacheable(ctx);
        }
    }

    @Override
    public void putIfAbsent(Query query, LRUQueryCache.CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
        assert query instanceof BoostQuery == false;
        assert query instanceof ConstantScoreQuery == false;
        // under a lock to make sure that mostRecentlyUsedQueries and cache remain sync'ed
        final IndexReader.CacheKey key = cacheHelper.getKey();
        QueryCacheKey queryCacheKey = new QueryCacheKey(key, query);
        int partitionNumber = queryCacheKey.hashCode() & (this.numberOfSegments - 1);
        //partitionNumberMap.computeIfAbsent(partitionNumber, k -> new AtomicInteger(0)).incrementAndGet();
        this.lruQueryCacheSegment[partitionNumber].putIfAbsent(queryCacheKey, query, cacheHelper, cached);
    }

    public final long getCacheCount() {
        return cacheCount.sum();
    }

    public final long getTotalCount() {
        return getHitCount() + getMissCount();
    }

    public final long getHitCount() {
        return hitCount.sum();
    }

    public final long getEvictionCount() {
        return getCacheCount() - getCacheSize();
    }

    public final long getCacheSize() {
        return cacheSize.sum();
    }

    /**
     * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
     * how many times this query was not contained in the cache.
     *
     * @see #getTotalCount()
     * @see #getHitCount()
     */
    public final long getMissCount() {
        return missCount.sum();
    }

    /**
     * Default cache implementation: uses {@link RoaringDocIdSet} for sets that have a density &lt; 1%
     * and a {@link BitDocIdSet} over a {@link FixedBitSet} otherwise.
     */
    protected LRUQueryCache.CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
        if (scorer.cost() * 100 >= maxDoc) {
            // FixedBitSet is faster for dense sets and will enable the random-access
            // optimization in ConjunctionDISI
            return cacheIntoBitSet(scorer, maxDoc);
        } else {
            return cacheIntoRoaringDocIdSet(scorer, maxDoc);
        }
    }

    private static LRUQueryCache.CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
        final FixedBitSet bitSet = new FixedBitSet(maxDoc);
        int[] count = new int[1];
        scorer.score(
            new LeafCollector() {

                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    count[0]++;
                    bitSet.set(doc);
                }
            },
            null,
            0,
            DocIdSetIterator.NO_MORE_DOCS);
        return new LRUQueryCache.CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
    }

    private static LRUQueryCache.CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc)
        throws IOException {
        RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
        scorer.score(
            new LeafCollector() {

                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    builder.add(doc);
                }
            },
            null,
            0,
            DocIdSetIterator.NO_MORE_DOCS);
        RoaringDocIdSet cache = builder.build();
        return new LRUQueryCache.CacheAndCount(cache, cache.cardinality());
    }

    /** Remove all cache entries for the given core cache key. */
    public void clearCoreCacheKey(IndexReader.CacheKey coreKey) {
        synchronized (keysToClean) {
            keysToClean.add(coreKey);
        }
    }
    /**
     * Expert: callback when a query is added to this cache. Implementing this method is typically
     * useful in order to compute more fine-grained statistics about the query cache.
     *
     * //@see #onQueryEviction
     * @lucene.experimental
     */
    protected void onQueryCache(Query query, long ramBytesUsed) {
        assert writeLock.isHeldByCurrentThread();
        this.ramBytesUsed.add(ramBytesUsed);
    }

    private static long getRamBytesUsed(Query query) {
        return LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
            + (query instanceof Accountable accountableQuery
            ? accountableQuery.ramBytesUsed()
            : QUERY_DEFAULT_RAM_BYTES_USED);
    }


    private void onEviction(Query singleton) {
        onQueryEviction(singleton, getRamBytesUsed(singleton));
        for (int i = 0; i < this.numberOfSegments; i++) {
            lruQueryCacheSegment[i].remove(singleton);
        }
    }

    /**
     * Expert: callback when a query is evicted from this cache.
     *
     * @see #onQueryCache
     * @lucene.experimental
     */
    protected void onQueryEviction(Query query, long ramBytesUsed) {
        this.ramBytesUsed.add(-ramBytesUsed);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed.sum();
    }
}
