package org.apache.lucene.benchmark.jmh;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.LRUQueryCacheV2;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Group)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 5,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class QueryCacheBenchmark {

    private static final int MAX_SIZE = 10000;
    private static final int MAX_SIZE_IN_BYTES = 1048576;

    private static final int SEGMENTS = 50;

    private  Query[] queries;
    private IndexReader.CacheHelper[] cacheHelpers;

    private LRUQueryCache.CacheAndCount sampleCacheAndCount;


    QueryCache queryCache;

    QueryCache queryCacheV2;
    @Setup
    public void setup() {
        queryCache = new LRUQueryCache(MAX_SIZE, MAX_SIZE_IN_BYTES, leafReaderContext -> true, 100000);

        queryCacheV2 = new LRUQueryCacheV2(MAX_SIZE, MAX_SIZE_IN_BYTES, leafReaderContext -> true, 100000, 16);

        this.queries = new Query[MAX_SIZE];
        for (int i = 0; i < MAX_SIZE; i++) {
            queries[i] = new TermQuery(new Term("field" + i, UUID.randomUUID().toString()));
        }
        this.cacheHelpers = new IndexReader.CacheHelper[SEGMENTS];
        for (int i = 0; i < SEGMENTS; i++) {
            this.cacheHelpers[i] = new IndexReader.CacheHelper() {

                private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();
                @Override
                public IndexReader.CacheKey getKey() {
                    return cacheKey;
                }

                @Override
                public void addClosedListener(IndexReader.ClosedListener listener) {

                }
            };
        }

        FixedBitSet bitSet = new FixedBitSet(10000);
        int cost = 0;
        int start = 10;
        final int interval = 12; // to keep it simple
        for (int i = 10; i < bitSet.length(); i += interval) {
            cost++;
            bitSet.set(i);
        }

        this.sampleCacheAndCount = new LRUQueryCache.CacheAndCount(new BitDocIdSet(bitSet,
            cost), cost); // Using same value for all cache entries.
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        ((LRUQueryCache) queryCache).close();
        ((LRUQueryCache) queryCacheV2).close();
    }


    @Benchmark
    @Group("concurrent_puts_v1")
    @GroupThreads(8)
    public void testConcurrentPuts() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        queryCache.putIfAbsent(queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
    }

    @Benchmark
    @Group("concurrent_puts_v2")
    @GroupThreads(8)
    public void testConcurrentPutsV2() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        queryCacheV2.putIfAbsent(queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
    }

    @Benchmark
    @Group("concurrentGetAndPuts")
    @GroupThreads(6)
    public LRUQueryCache.CacheAndCount concurrentGetAndPuts_get() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        return queryCache.get(queries[random], cacheHelpers[random & (SEGMENTS - 1)]);
    }

    @Benchmark
    @Group("concurrentGetAndPuts")
    @GroupThreads(4)
    public void concurrentGetAndPuts_put() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        queryCache.putIfAbsent(queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
    }

    @Benchmark
    @Group("concurrentGetAndPuts_v2")
    @GroupThreads(6)
    public LRUQueryCache.CacheAndCount concurrentGetAndPuts_getV2() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        return queryCacheV2.get(queries[random], cacheHelpers[random & (SEGMENTS - 1)]);
    }

    @Benchmark
    @Group("concurrentGetAndPuts_v2")
    @GroupThreads(4)
    public void concurrentGetAndPuts_putV2() {
        int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
        queryCacheV2.putIfAbsent(queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
    }
}
