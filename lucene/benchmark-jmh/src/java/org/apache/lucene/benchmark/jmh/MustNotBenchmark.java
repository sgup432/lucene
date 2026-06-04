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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark for BooleanQuery MUST_NOT performance with varying numbers of excluded terms.
 *
 * <p>Replicates the regression observed when upgrading from Lucene 9 to Lucene 10 where queries
 * with many interleaved MUST_NOT term clauses on a keyword field show significantly degraded
 * performance due to the docIDRunEnd() optimization in ReqExclBulkScorer triggering expensive
 * computeTopList() calls in DisjunctionDISIApproximation.
 *
 * <p>Two scenarios are benchmarked:
 * <ul>
 *   <li>INTERLEAVED: excluded terms are spread evenly across the index (worst case for docIDRunEnd)
 *   <li>DENSE: excluded terms form long contiguous runs (best case for docIDRunEnd)
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx4g", "-Xms4g", "-XX:+AlwaysPreTouch"})
public class MustNotBenchmark {

  // Number of MUST_NOT term clauses — 1 is the "works well" case, 11 replicates the regression
  @Param({"1", "11"})
  public int numExcludedTerms;

  // INTERLEAVED: excluded types spread evenly (triggers regression)
  // DENSE: excluded types form contiguous blocks (benefits from docIDRunEnd)
  @Param({"INTERLEAVED", "DENSE"})
  public String distribution;

  private static final int NUM_DOCS = 10_000_000;
  private static final String FIELD = "category";
  private static final String REQUIRED_VALUE = "required_type";

  // Excluded type values — mirrors the customer's 11 must_not types
  private static final String[] ALL_EXCLUDED_VALUES = {
    "type_a", "type_b", "type_c", "type_d", "type_e",
    "type_f", "type_g", "type_h", "type_i", "type_j", "type_k"
  };

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;
  private BooleanQuery query;
  private Path tmpDir;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    tmpDir = Files.createTempDirectory("must_not_bench");
    dir = MMapDirectory.open(tmpDir);

    IndexWriterConfig config = new IndexWriterConfig();
    config.setMaxBufferedDocs(NUM_DOCS + 1);

    try (IndexWriter writer = new IndexWriter(dir, config)) {
      String[] excludedValues = getActiveExcludedValues();
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(new StringField(FIELD, assignCategory(i, excludedValues), Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
    query = buildQuery();
  }

  /**
   * INTERLEAVED: ~90% excluded, cycling round-robin across K terms every 10 docs.
   *   Run length for the disjunction = 1 (next excluded doc is always a different type).
   *
   * DENSE: ~90% excluded in K contiguous blocks, ~10% required at the end.
   *   Run length per type = ~900K docs — docIDRunEnd() can skip entire blocks.
   */
  private String assignCategory(int docId, String[] excludedValues) {
    int k = excludedValues.length;
    if ("INTERLEAVED".equals(distribution)) {
      if (docId % 10 == 0) return REQUIRED_VALUE;
      return excludedValues[(docId % (k * 9)) / 9];
    } else {
      int excludedCount = (int) (NUM_DOCS * 0.9);
      if (docId >= excludedCount) return REQUIRED_VALUE;
      int blockSize = excludedCount / k;
      return excludedValues[Math.min(docId / blockSize, k - 1)];
    }
  }

  private String[] getActiveExcludedValues() {
    String[] result = new String[numExcludedTerms];
    System.arraycopy(ALL_EXCLUDED_VALUES, 0, result, 0, numExcludedTerms);
    return result;
  }

  private BooleanQuery buildQuery() {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    for (String value : getActiveExcludedValues()) {
      builder.add(new TermQuery(new Term(FIELD, value)), BooleanClause.Occur.MUST_NOT);
    }
    return builder.build();
  }

  /**
   * Forces full doc-by-doc traversal — no count() shortcut.
   * Uses a simple counting collector that cannot be optimized away.
   */
  @Benchmark
  public int mustNotQuery() throws IOException {
    CountingCollector collector = new CountingCollector();
    searcher.search(query, collector);
    return collector.count;
  }

  /**
   * Simple collector that counts hits without any shortcut paths.
   * Unlike TotalHitCountCollector, this does NOT call weight.count() and
   * forces the scorer to iterate every matching document.
   */
  private static class CountingCollector implements Collector {
    int count = 0;

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) {
      return new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
          count++;
        }
      };
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    IOUtils.close(reader, dir);
    Files.walk(tmpDir)
        .sorted(java.util.Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(java.io.File::delete);
  }
}
