///// *
//// * Licensed to the Apache Software Foundation (ASF) under one or more
//// * contributor license agreements.  See the NOTICE file distributed with
//// * this work for additional information regarding copyright ownership.
//// * The ASF licenses this file to You under the Apache License, Version 2.0
//// * (the "License"); you may not use this file except in compliance with
//// * the License.  You may obtain a copy of the License at
//// *
//// *     http://www.apache.org/licenses/LICENSE-2.0
//// *
//// * Unless required by applicable law or agreed to in writing, software
//// * distributed under the License is distributed on an "AS IS" BASIS,
//// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// * See the License for the specific language governing permissions and
//// * limitations under the License.
//// */
// package org.apache.lucene.benchmark.jmh;
//
// import java.io.File;
// import java.io.IOException;
// import java.nio.file.Path;
// import java.text.ParseException;
// import java.util.concurrent.ThreadLocalRandom;
// import jdk.jfr.Configuration;
// import jdk.jfr.Recording;
// import jdk.jfr.RecordingState;
// import org.apache.lucene.index.IndexReader;
// import org.apache.lucene.index.Term;
// import org.apache.lucene.search.BooleanClause;
// import org.apache.lucene.search.BooleanQuery;
// import org.apache.lucene.search.LRUQueryCache;
// import org.apache.lucene.search.Query;
// import org.apache.lucene.search.TermQuery;
// import org.apache.lucene.util.BitDocIdSet;
// import org.apache.lucene.util.FixedBitSet;
// import org.apache.lucene.util.SuppressForbidden;
// import org.openjdk.jmh.annotations.Benchmark;
// import org.openjdk.jmh.annotations.Fork;
// import org.openjdk.jmh.annotations.Group;
// import org.openjdk.jmh.annotations.GroupThreads;
// import org.openjdk.jmh.annotations.Level;
// import org.openjdk.jmh.annotations.Measurement;
// import org.openjdk.jmh.annotations.Scope;
// import org.openjdk.jmh.annotations.Setup;
// import org.openjdk.jmh.annotations.State;
// import org.openjdk.jmh.annotations.TearDown;
// import org.openjdk.jmh.annotations.Warmup;
//
// @State(Scope.Group)
// @Warmup(iterations = 3, time = 5)
// @Measurement(iterations = 3, time = 10)
// @Fork(
//    value = 3,
//    jvmArgsAppend = {"-Xmx8g", "-Xms8g", "-XX:+AlwaysPreTouch"})
// @SuppressForbidden(reason = "Suppress")
// public class QueryCacheBenchmark {
//
//  private static final int MAX_SIZE = 10000;
//  private static final int MAX_SIZE_IN_BYTES = 1048576;
//
//  private static final int SEGMENTS = 50;
//
//  private Query[] queries;
//  private IndexReader.CacheHelper[] cacheHelpers;
//
//  private LRUQueryCache.CacheAndCount sampleCacheAndCount;
//
//  LRUQueryCache queryCache;
//
//  Path jfrFile;
//  Recording recording;
//
//  //  @State(Scope.Thread)
//  //  public static class ThreadState {
//  //    static final Random random = new Random();
//  //    int index = random.nextInt();
//  //  }
//
//  @Setup
//  public void setup() throws IOException {
//    queryCache = new LRUQueryCache(MAX_SIZE, MAX_SIZE_IN_BYTES, _ -> true, 100000);
//
//    this.queries = new Query[MAX_SIZE];
//    for (int i = 0; i < MAX_SIZE; i++) {
//      TermQuery must = new TermQuery(new Term("foo", "bar" + i));
//      TermQuery filter = new TermQuery(new Term("foo", "quux" + i));
//      TermQuery mustNot = new TermQuery(new Term("foo", "foo" + i));
//      BooleanQuery.Builder bq = new BooleanQuery.Builder();
//      bq.add(must, BooleanClause.Occur.FILTER);
//      bq.add(filter, BooleanClause.Occur.FILTER);
//      bq.add(mustNot, BooleanClause.Occur.MUST_NOT);
//      queries[i] = bq.build();
//    }
//
//    this.cacheHelpers = new IndexReader.CacheHelper[SEGMENTS];
//    for (int i = 0; i < SEGMENTS; i++) {
//      this.cacheHelpers[i] =
//          new IndexReader.CacheHelper() {
//
//            private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();
//
//            @Override
//            public IndexReader.CacheKey getKey() {
//              return cacheKey;
//            }
//
//            @Override
//            public void addClosedListener(IndexReader.ClosedListener listener) {}
//          };
//    }
//
//    FixedBitSet bitSet = new FixedBitSet(10000);
//    int cost = 0;
//    final int interval = 12; // to keep it simple
//    for (int i = 10; i < bitSet.length(); i += interval) {
//      cost++;
//      bitSet.set(i);
//    }
//
//    this.sampleCacheAndCount =
//        new LRUQueryCache.CacheAndCount(
//            new BitDocIdSet(bitSet, cost), cost); // Using same value for all cache entries.
//    this.jfrFile = new
// File("/Users/upasagar/Downloads/test-recording-query-cache-1.jfr").toPath();
//    try {
//      this.recording = new Recording(Configuration.getConfiguration("profile"));
//    } catch (ParseException e) {
//      throw new RuntimeException(e);
//    }
//    //    this.recording = new Recording();
//    //    // Configure recording
//    //    recording.enable("jdk.CPULoad");
//    //    recording.enable("jdk.ThreadAllocationStatistics"); // example: CPU load
//    //    recording.enable("jdk.JavaMonitorEnter"); // monitor contention
//    //    recording.enable("jdk.ExecutionSample");
//    //    recording.enable("jdk.ThreadPark");
//    //    recording.enable("jdk.MethodSample");
//    //    recording.enable("jdk.ThreadDump"); // Full thread dump
//    //    recording.enable("jdk.JavaThreadStatistics");
//    //    recording.enable("jdk.GarbageCollection"); // GC pauses, cause, duration
//    //    recording.enable("jdk.GCHeapSummary"); // Heap usage before/after GC
//    //    recording.enable("jdk.GCHeapConfiguration"); // Heap/region configuration
//    //    recording.enable("jdk.GCPhasePause"); // Pause sub-phases
//    //    recording.enable("jdk.MetaspaceSummary"); // Metaspace usage
//    //    recording.enable("jdk.ObjectCount"); // Object counts by class (sampled)
//    //    recording.enable("jdk.YoungGeneration"); // Young gen usage
//    //    recording.enable("jdk.OldObjectSample");
//    //    recording.enable("jdk.JavaMonitorWait");
//    recording.setToDisk(true);
//    recording.start();
//
//    //    for (int i = 0; i < MAX_SIZE; i++) {
//    //      queryCache.putIfAbsent(
//    //          queries[i], this.sampleCacheAndCount, cacheHelpers[i & (SEGMENTS - 1)]);
//    //    }
//    System.out.println("cache size = " + queryCache.getCacheSize());
//  }
//
//  @TearDown(Level.Trial)
//  public void tearDown() {
//    if (recording != null && recording.getState() == RecordingState.RUNNING) {
//      recording.stop();
//      try {
//        Path jfrFile2 =
//            Path.of(
//                "/Users/upasagar/Downloads/benchmark-custom-"
//                    + System.currentTimeMillis()
//                    + ".jfr");
//        recording.dump(jfrFile2);
//        System.out.println("JFR recording dumped to " + jfrFile2.toAbsolutePath());
//      } catch (IOException e) {
//        e.printStackTrace();
//      } finally {
//        recording.close();
//      }
//    }
//  }
//
//  @TearDown(Level.Iteration)
//  public void iterationTearDown() {
//    System.out.println("Here with stats -----");
//    System.out.println(
//        "Hit = "
//            + queryCache.getHitCount()
//            + " miss = "
//            + queryCache.getMissCount()
//            + " number of queries = "
//            + queryCache.cachedQueries().size()
//            + " eviction count = "
//            + queryCache.getEvictionCount()
//            + " size in bytes = "
//            + queryCache.ramBytesUsed());
//    //    for (Query query : queryCache.cachedQueries()) {
//    //      System.out.println("query = " + query.toString());
//    //    }
//    queryCache.clear();
//    //    //    for (int i = 0; i < MAX_SIZE; i++) {
//    //    //      queryCache.putIfAbsent(
//    //          queries[i], this.sampleCacheAndCount, cacheHelpers[(i + 1) & (SEGMENTS - 1)]);
//    //    }
//  }
//
//  //  @Benchmark
//  //  @Group("concurrentGetOnly")
//  //  @GroupThreads(10)
//  //  public LRUQueryCache.CacheAndCount testConcurrentGets() {
//  //    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//  //    return queryCache.get(queries[random], cacheHelpers[random & (SEGMENTS - 1)]);
//  //  }
//
//  //  @Benchmark
//  //  @Group("concurrentPutOnly")
//  //  @GroupThreads(10)
//  //  public void testConcurrentPuts() {
//  //    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//  //    queryCache.putIfAbsent(
//  //        queries[random], this.sampleCacheAndCount, cacheHelpers[random & (SEGMENTS - 1)]);
//  //  }
//
//  @Benchmark
//  @Group("concurrentGetAndPuts")
//  @GroupThreads(6)
//  public LRUQueryCache.CacheAndCount concurrentGetAndPuts_get() {
//    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//    return queryCache.get(queries[random & (MAX_SIZE - 1)], cacheHelpers[random & (SEGMENTS -
// 1)]);
//  }
//
//  @Benchmark
//  @Group("concurrentGetAndPuts")
//  @GroupThreads(4)
//  public void concurrentGetAndPuts_put() {
//    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//    queryCache.putIfAbsent(
//        queries[random & (MAX_SIZE - 1)],
//        this.sampleCacheAndCount,
//        cacheHelpers[random & (SEGMENTS - 1)]);
//  }
//
//  //  @Benchmark
//  //  @Group("concurrentGetAndPuts_v2")
//  //  @GroupThreads(6)
//  //  public LRUQueryCache.CacheAndCount concurrentGetAndPuts_getV2() {
//  //    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//  //    return queryCacheV2.get(queries[random], cacheHelpers[random & (SEGMENTS - 1)]);
//  //  }
//
//  //  @Benchmark
//  //  @Group("concurrentGetAndPuts_v2")
//  //  @GroupThreads(4)
//  //  public void concurrentGetAndPuts_putV2() {
//  //    int random = ThreadLocalRandom.current().nextInt(MAX_SIZE);
//  //    queryCacheV2.putIfAbsent(queries[random], this.sampleCacheAndCount, cacheHelpers[random &
//  // (SEGMENTS - 1)]);
//  //  }
// }
