/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * A {@link QueryCache} that evicts queries using a LRU (least-recently-used) eviction policy in
 * order to remain under a given maximum size and number of bytes used.
 *
 * <p>This class is thread-safe.
 *
 * <p>Note that query eviction runs in linear time with the total number of segments that have cache
 * entries so this cache works best with {@link QueryCachingPolicy caching policies} that only cache
 * on "large" segments, and it is advised to not share this cache across too many indices.
 *
 * <p>A default query cache and policy instance is used in IndexSearcher. If you want to replace
 * those defaults it is typically done like this:
 *
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count}, {@link
 * #getMissCount() miss count}, {@link #getCacheSize() number of cache entries}, {@link
 * #getCacheCount() total number of DocIdSets that have ever been cached}, {@link
 * #getEvictionCount() number of evicted entries}). In case you would like to have more fine-grained
 * statistics, such as per-index or per-query-class statistics, it is possible to override various
 * callbacks: {@link #onHit}, {@link #onMiss}, {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}. It is better to not
 * perform heavy computations in these methods though since they are called synchronously and under
 * a lock.
 *
 * @see QueryCachingPolicy
 * @lucene.experimental
 */
public class LRUQueryCache implements QueryCache, Accountable {

  private final int maxSize;
  private final long maxRamBytesUsed;
  private final Predicate<LeafReaderContext> leavesToCache;
  private final Map<IndexReader.CacheKey, LeafCache> cache;
  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;
  private volatile float skipCacheFactor;
  private final LongAdder hitCount;
  private final LongAdder missCount;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile LongAdder ramBytesUsed;
  private volatile long cacheCount;
  private volatile long cacheSize;
  // This lock is used to perform any mutable operations to the custom LRU list
  private final ReentrantLock lruLock = new ReentrantLock();
  // Defines the head and tail of the LRU list where the head contains the least recently used entry
  private QueryNode head, tail;
  // A map to track the queries in the LRU list
  Map<Query, QueryNode> queries;
  // We maintain an explicit query count to decide whether we want to evict items or not which
  // happens under a lru lock.
  private volatile int queryCount;

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code> queries with at most
   * <code>maxRamBytesUsed</code> bytes of memory, only on leaves that satisfy {@code
   * leavesToCache}.
   *
   * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
   * top-level query will not be cached in order to not slow down queries too much.
   */
  public LRUQueryCache(
      int maxSize,
      long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache,
      float skipCacheFactor) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    this.leavesToCache = leavesToCache;
    if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
      throw new IllegalArgumentException(
          "skipCacheFactor must be no less than 1, get " + skipCacheFactor);
    }
    this.skipCacheFactor = skipCacheFactor;
    this.queries = new HashMap<>();
    cache = new IdentityHashMap<>();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    writeLock = lock.writeLock();
    readLock = lock.readLock();
    ramBytesUsed = new LongAdder();
    hitCount = new LongAdder();
    missCount = new LongAdder();
  }

  // The state of an entry in the LRU list
  enum State {
    NEW,
    EXISTING,
    DELETED
  }

  private boolean removeEntry(QueryNode entry) {
    assert lruLock.isHeldByCurrentThread();
    if (entry.state == State.EXISTING) {
      QueryNode before = entry.prev;
      QueryNode after = entry.next;

      if (before == null) {
        assert head == entry;
        head = after;
        if (head != null) {
          head.prev = null;
        }
      } else {
        before.next = after;
        entry.prev = null;
      }

      if (after == null) {
        // removing tail
        assert tail == entry;
        tail = before;
        if (tail != null) {
          tail.next = null;
        }
      } else {
        after.prev = before;
        entry.next = null;
      }

      this.onQueryEviction(entry.query, getRamBytesUsed(entry.query));
      entry.state = State.DELETED;
      return true;
    } else {
      return false;
    }
  }

  private void moveToTail(QueryNode entry) {
    assert lruLock.isHeldByCurrentThread();
    if (tail == entry) {
      return;
    }
    removeEntry(entry);
    addToTail(entry);
  }

  private void addToTail(QueryNode entry) {
    assert lruLock.isHeldByCurrentThread();
    entry.prev = tail;
    entry.next = null;
    if (tail != null) {
      tail.next = entry;
    }
    tail = entry;
    if (head == null) {
      head = entry;
    }
    this.onQueryCache(entry.query, getRamBytesUsed(entry.query));
    entry.state = State.EXISTING;
  }

  private List<Query> promote(QueryNode entry) {
    List<Query> evictedQueries;
    if (entry == null) {
      return new ArrayList<>();
    }
    try {
      lruLock.lock();
      switch (entry.state) {
        case NEW:
          addToTail(entry);
          break;
        case DELETED:
          break;
        case EXISTING:
          moveToTail(entry);
          break;
      }
      evictedQueries = evictIfNecessary();
    } finally {
      lruLock.unlock();
    }
    return evictedQueries;
  }

  /**
   * Get the skip cache factor
   *
   * @return #setSkipCacheFactor
   */
  public float getSkipCacheFactor() {
    return skipCacheFactor;
  }

  /**
   * This setter enables the skipCacheFactor to be updated dynamically.
   *
   * @param skipCacheFactor clauses whose cost is {@code skipCacheFactor} times more than the cost
   *     of the top-level query will not be cached in order to not slow down queries too much.
   */
  public void setSkipCacheFactor(float skipCacheFactor) {
    this.skipCacheFactor = skipCacheFactor;
  }

  /**
   * Create a new instance that will cache at most <code>maxSize</code> queries with at most <code>
   * maxRamBytesUsed</code> bytes of memory. Queries will only be cached on leaves that have more
   * than 10k documents and have more than half of the average documents per leave of the index.
   * This should guarantee that all leaves from the upper {@link TieredMergePolicy tier} will be
   * cached. Only clauses whose cost is at most 100x the cost of the top-level query will be cached
   * in order to not hurt latency too much because of caching.
   */
  public LRUQueryCache(int maxSize, long maxRamBytesUsed) {
    this(maxSize, maxRamBytesUsed, new MinSegmentSizePredicate(10000), 10);
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
   * Expert: callback when there is a cache hit on a given query. Implementing this method is
   * typically useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onMiss
   * @lucene.experimental
   */
  protected void onHit(Object readerCoreKey, Query query) {
    hitCount.add(1);
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

  /**
   * Expert: callback when a query is added to this cache. Implementing this method is typically
   * useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onQueryEviction
   * @lucene.experimental
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {
    assert lruLock.isHeldByCurrentThread();
    this.ramBytesUsed.add(ramBytesUsed);
    this.queryCount += 1;
  }

  /**
   * Expert: callback when a query is evicted from this cache.
   *
   * @see #onQueryCache
   * @lucene.experimental
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {
    assert lruLock.isHeldByCurrentThread();
    this.ramBytesUsed.add(-ramBytesUsed);
    this.queryCount -= 1;
  }

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache. Implementing this method is
   * typically useful in order to compute more fine-grained statistics about the query cache.
   *
   * @see #onDocIdSetEviction
   * @lucene.experimental
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
    assert writeLock.isHeldByCurrentThread();
    cacheSize += 1;
    cacheCount += 1;
    this.ramBytesUsed.add(ramBytesUsed);
  }

  /**
   * Expert: callback when one or more {@link DocIdSet}s are removed from this cache.
   *
   * @see #onDocIdSetCache
   * @lucene.experimental
   */
  protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
    assert writeLock.isHeldByCurrentThread();
    this.ramBytesUsed.add(-sumRamBytesUsed);
    cacheSize -= numEntries;
  }

  /**
   * Expert: callback when the cache is completely cleared.
   *
   * @lucene.experimental
   */
  protected void onClear() {
    assert writeLock.isHeldByCurrentThread();
    ramBytesUsed = new LongAdder();
    cacheSize = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    assert lruLock.isHeldByCurrentThread();
    final int size = this.queryCount;
    if (size == 0) {
      return false;
    } else {
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  CacheAndCount get(Query key, IndexReader.CacheHelper cacheHelper) {
    assert key instanceof BoostQuery == false;
    assert key instanceof ConstantScoreQuery == false;
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    final LeafCache leafCache = cache.get(readerKey);
    if (leafCache == null) {
      onMiss(readerKey, key);
      return null;
    }
    final QueryNode queryNode = queries.get(key);
    if (queryNode == null || queryNode.query == null) {
      onMiss(readerKey, key);
      return null;
    }
    key = queryNode.query;
    final CacheAndCount cached = leafCache.get(key);
    if (cached == null) {
      onMiss(readerKey, key);
    } else {
      onHit(readerKey, key);
    }
    return cached;
  }

  private void putIfAbsent(Query query, CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    QueryNode entry;
    writeLock.lock();
    try {
      if (queries.get(query) == null) {
        entry = new QueryNode(query);
        queries.put(query, entry);
      } else {
        entry = queries.get(query);
      }
      query = entry.query;
      final IndexReader.CacheKey key = cacheHelper.getKey();
      LeafCache leafCache = cache.get(key);
      if (leafCache == null) {
        leafCache = new LeafCache(key);
        final LeafCache previous = cache.put(key, leafCache);
        ramBytesUsed.add(HASHTABLE_RAM_BYTES_PER_ENTRY);
        assert previous == null;
        // we just created a new leaf cache, need to register a close listener
        cacheHelper.addClosedListener(this::clearCoreCacheKey);
      }
      leafCache.putIfAbsent(query, cached);
    } finally {
      writeLock.unlock();
    }
    List<Query> evictedQueries;
    lruLock.lock();
    try {
      evictedQueries = promote(entry);
    } finally {
      lruLock.unlock();
    }
    if (!evictedQueries.isEmpty()) {
      evictQueries(evictedQueries);
    }
  }

  private void evictQueries(List<Query> evictedQueries) {
    writeLock.lock();
    try {
      for (Query query : evictedQueries) {
        queries.remove(query);
        onEviction(query);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private List<Query> evictIfNecessary() {
    assert lruLock.isHeldByCurrentThread();
    List<Query> evictedQueries = new ArrayList<>();
    while (head != null && requiresEviction()) {
      QueryNode entry = head;
      removeEntry(entry);
      evictedQueries.add(entry.query);
    }
    return evictedQueries;
  }

  /** Remove all cache entries for the given core cache key. */
  public void clearCoreCacheKey(Object coreKey) {
    writeLock.lock();
    try {
      final LeafCache leafCache = cache.remove(coreKey);
      if (leafCache != null) {
        ramBytesUsed.add(-HASHTABLE_RAM_BYTES_PER_ENTRY);
        final int numEntries = leafCache.cache.size();
        if (numEntries > 0) {
          onDocIdSetEviction(coreKey, numEntries, leafCache.ramBytesUsed);
        } else {
          assert numEntries == 0;
          assert leafCache.ramBytesUsed == 0;
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /** Remove all cache entries for the given query. */
  public void clearQuery(Query query) {
    QueryNode removedQueryNode;
    writeLock.lock();
    try {
      removedQueryNode = queries.remove(query);
      if (removedQueryNode != null && removedQueryNode.query != null) {
        onEviction(removedQueryNode.query);
      }
    } finally {
      writeLock.unlock();
    }
    lruLock.lock();
    try {
      if (removedQueryNode != null) {
        removeEntry(removedQueryNode);
      }
    } finally {
      lruLock.unlock();
    }
  }

  private void onEviction(Query singleton) {
    assert writeLock.isHeldByCurrentThread();
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /** Clear the content of this cache. */
  public void clear() {
    writeLock.lock();
    try {
      cache.clear();
      queries.clear();

      onClear();
    } finally {
      writeLock.unlock();
    }
    try {
      lruLock.lock();
      QueryNode current = head;
      while (current != null) {
        current.state = State.DELETED;
        current = current.next;
      }
      head = tail = null;
      this.queryCount = 0;
    } finally {
      lruLock.unlock();
    }
  }

  private static long getRamBytesUsed(Query query) {
    return LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
        + (query instanceof Accountable accountableQuery
            ? accountableQuery.ramBytesUsed()
            : QUERY_DEFAULT_RAM_BYTES_USED);
  }

  // pkg-private for testing
  void assertConsistent() {
    lruLock.lock();
    try {
      if (requiresEviction()) {
        throw new AssertionError(
            "requires evictions: size="
                + this.queryCount
                + ", maxSize="
                + maxSize
                + ", ramBytesUsed="
                + ramBytesUsed()
                + ", maxRamBytesUsed="
                + maxRamBytesUsed);
      }
    } finally {
      lruLock.unlock();
    }
    writeLock.lock();
    try {
      for (LeafCache leafCache : cache.values()) {
        Set<Query> keys = Collections.newSetFromMap(new IdentityHashMap<>());
        keys.addAll(leafCache.cache.keySet());
        keys.removeAll(cachedQueries());
        if (!keys.isEmpty()) {
          throw new AssertionError(
              "One leaf cache contains more keys than the top-level cache: " + keys);
        }
      }
      long recomputedRamBytesUsed = HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size();
      for (Query query : cachedQueries()) {
        recomputedRamBytesUsed += getRamBytesUsed(query);
      }
      for (LeafCache leafCache : cache.values()) {
        recomputedRamBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY * leafCache.cache.size();
        for (CacheAndCount entry : leafCache.cache.values()) {
          recomputedRamBytesUsed += entry.ramBytesUsed();
        }
      }
      if (recomputedRamBytesUsed != ramBytesUsed.longValue()) {
        throw new AssertionError(
            "ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
      }

      long recomputedCacheSize = 0;
      for (LeafCache leafCache : cache.values()) {
        recomputedCacheSize += leafCache.cache.size();
      }
      if (recomputedCacheSize != getCacheSize()) {
        throw new AssertionError(
            "cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private class CacheKeyIterator implements Iterator<QueryNode> {
    private QueryNode current;
    private QueryNode next;

    CacheKeyIterator(QueryNode head) {
      current = null;
      next = head;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public QueryNode next() {
      current = next;
      next = next.next;
      return current;
    }

    @Override
    public void remove() {
      QueryNode entry = current;
      if (entry != null) {
        lruLock.lock();
        try {
          current = null;
          removeEntry(entry);
        } finally {
          lruLock.unlock();
        }
        writeLock.lock();
        try {
          queries.remove(entry.query);
          onEviction(entry.query);
        } finally {
          writeLock.unlock();
        }
      }
    }
  }

  Iterable<Query> cachedKeys() {
    return () ->
        new Iterator<>() {
          private CacheKeyIterator iterator = new CacheKeyIterator(head);

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Query next() {
            return iterator.next().query;
          }

          @Override
          public void remove() {
            iterator.remove();
          }
        };
  }

  // pkg-private for testing
  // return the list of cached queries in LRU order
  List<Query> cachedQueries() {
    List<Query> queries = new ArrayList<>();
    lruLock.lock();
    try {
      for (Query query : cachedKeys()) {
        queries.add(query);
      }
    } finally {
      lruLock.unlock();
    }
    return queries;
  }

  @Override
  public Weight doCache(Weight weight, QueryCachingPolicy policy) {
    while (weight instanceof CachingWrapperWeight) {
      weight = ((CachingWrapperWeight) weight).in;
    }

    return new CachingWrapperWeight(weight, policy);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.sum();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    writeLock.lock();
    try {
      return Accountables.namedAccountables("segment", cache);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet} for sets that have a density &lt; 1%
   * and a {@link BitDocIdSet} over a {@link FixedBitSet} otherwise.
   */
  protected CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
    if (scorer.cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      return cacheIntoBitSet(scorer, maxDoc);
    } else {
      return cacheIntoRoaringDocIdSet(scorer, maxDoc);
    }
  }

  private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
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
    return new CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
  }

  private static CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc)
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
    return new CacheAndCount(cache, cache.cardinality());
  }

  /**
   * Return the total number of times that a {@link Query} has been looked up in this {@link
   * QueryCache}. Note that this number is incremented once per segment so running a cached query
   * only once will increment this counter by the number of segments that are wrapped by the
   * searcher. Note that by definition, {@link #getTotalCount()} is the sum of {@link
   * #getHitCount()} and {@link #getMissCount()}.
   *
   * @see #getHitCount()
   * @see #getMissCount()
   */
  public final long getTotalCount() {
    return getHitCount() + getMissCount();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
   * how many times a cached {@link DocIdSet} has been found and returned.
   *
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount.sum();
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
   * Return the total number of {@link DocIdSet}s which are currently stored in the cache.
   *
   * @see #getCacheCount()
   * @see #getEvictionCount()
   */
  public final long getCacheSize() {
    return cacheSize;
  }

  /**
   * Return the total number of cache entries that have been generated and put in the cache. It is
   * highly desirable to have a {@link #getHitCount() hit count} that is much higher than the {@link
   * #getCacheCount() cache count} as the opposite would indicate that the query cache makes efforts
   * in order to cache queries but then they do not get reused.
   *
   * @see #getCacheSize()
   * @see #getEvictionCount()
   */
  public final long getCacheCount() {
    return cacheCount;
  }

  /**
   * Return the number of cache entries that have been removed from the cache either in order to
   * stay under the maximum configured size/ram usage, or because a segment has been closed. High
   * numbers of evictions might mean that queries are not reused or that the {@link
   * QueryCachingPolicy caching policy} caches too aggressively on NRT segments which get merged
   * early.
   *
   * @see #getCacheCount()
   * @see #getCacheSize()
   */
  public final long getEvictionCount() {
    return getCacheCount() - getCacheSize();
  }

  /** Represents an entry in the LRU-ordered doubly linked list */
  class QueryNode {
    Query query;
    State state;
    QueryNode prev, next;

    QueryNode(Query query) {
      this.query = query;
      state = State.NEW;
    }

    @Override
    public String toString() {
      return "Query: " + query.toString() + ", State: " + state.toString();
    }
  }

  // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
  private class LeafCache implements Accountable {

    private final Object key;
    private final Map<Query, CacheAndCount> cache;
    private volatile long ramBytesUsed;

    LeafCache(Object key) {
      this.key = key;
      cache = new IdentityHashMap<>();
      ramBytesUsed = 0;
    }

    private void onDocIdSetCache(long ramBytesUsed) {
      assert writeLock.isHeldByCurrentThread();
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onDocIdSetCache(key, ramBytesUsed);
    }

    private void onDocIdSetEviction(long ramBytesUsed) {
      assert writeLock.isHeldByCurrentThread();
      this.ramBytesUsed -= ramBytesUsed;
      LRUQueryCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
    }

    CacheAndCount get(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      if (cache.get(query) == null) {
        return null;
      } else {
        return cache.get(query);
      }
    }

    void putIfAbsent(Query query, CacheAndCount cached) {
      assert writeLock.isHeldByCurrentThread();
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      if (cache.putIfAbsent(query, cached) == null) {
        // the set was actually put
        onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + cached.ramBytesUsed());
      }
    }

    void remove(Query query) {
      assert writeLock.isHeldByCurrentThread();
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      CacheAndCount removed = cache.remove(query);
      if (removed != null) {
        onDocIdSetEviction(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed());
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  private class CachingWrapperWeight extends ConstantScoreWeight {

    private final Weight in;
    private final QueryCachingPolicy policy;
    // we use an AtomicBoolean because Weight.scorer may be called from multiple
    // threads when IndexSearcher is created with threads
    private final AtomicBoolean used;

    CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
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
      // Imagine the worst-case that a cache entry is large than the size of
      // the cache: not only will this entry be trashed immediately but it
      // will also evict all current entries from the cache. For this reason
      // we only cache on an IndexReader if we have available room for
      // 5 different filters on this reader to avoid excessive trashing
      return worstCaseRamUsage * 5 < maxRamBytesUsed;
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

      CacheAndCount cached;
      try {
        cached = get(in.getQuery(), cacheHelper);
      } finally {
        readLock.unlock();
      }

      int maxDoc = context.reader().maxDoc();
      if (cached == null) {
        if (policy.shouldCache(in.getQuery())) {
          final ScorerSupplier supplier = in.scorerSupplier(context);
          if (supplier == null) {
            putIfAbsent(in.getQuery(), CacheAndCount.EMPTY, cacheHelper);
            return null;
          }

          final long cost = supplier.cost();
          return new ConstantScoreScorerSupplier(0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc) {
            @Override
            public DocIdSetIterator iterator(long leadCost) throws IOException {
              // skip cache operation which would slow query down too much
              if (cost / skipCacheFactor > leadCost) {
                return supplier.get(leadCost).iterator();
              }

              CacheAndCount cached = cacheImpl(supplier.bulkScorer(), maxDoc);
              putIfAbsent(in.getQuery(), cached, cacheHelper);
              DocIdSetIterator disi = cached.iterator();
              if (disi == null) {
                // docIdSet.iterator() is allowed to return null when empty but we want a non-null
                // iterator here
                disi = DocIdSetIterator.empty();
              }

              return disi;
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
      // Promote the cached entry to the head of LRU list
      List<Query> evictedQueries = promote(queries.get(in.getQuery()));
      if (!evictedQueries.isEmpty()) {
        evictQueries(evictedQueries);
      }
      assert cached != null;
      if (cached == CacheAndCount.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = cached.iterator();
      if (disi == null) {
        return null;
      }

      return ConstantScoreScorerSupplier.fromIterator(
          disi, 0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc);
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

      CacheAndCount cached;
      try {
        cached = get(in.getQuery(), cacheHelper);
      } finally {
        readLock.unlock();
      }
      if (cached != null) {
        // Promote the cached entry to the head of LRU list
        List<Query> evictedQueries = promote(queries.get(in.getQuery()));
        if (!evictedQueries.isEmpty()) {
          evictQueries(evictedQueries);
        }
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

  /** Cache of doc ids with a count. */
  protected static class CacheAndCount implements Accountable {
    protected static final CacheAndCount EMPTY = new CacheAndCount(DocIdSet.EMPTY, 0);

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(CacheAndCount.class);
    private final DocIdSet cache;
    private final int count;

    public CacheAndCount(DocIdSet cache, int count) {
      this.cache = cache;
      this.count = count;
    }

    public DocIdSetIterator iterator() throws IOException {
      return cache.iterator();
    }

    public int count() {
      return count;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
    }
  }
}
