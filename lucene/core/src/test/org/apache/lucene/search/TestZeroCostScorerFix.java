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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Verifies the fix for https://github.com/apache/lucene/issues/15887:
 * Skip scorer construction for zero-cost SHOULD clauses in BooleanScorerSupplier.
 *
 * This test builds a pure disjunction (SHOULD-only) with a mix of matching
 * and non-matching clauses, then measures scorer construction time.
 * The fix in opt() skips clauses with cost()==0, avoiding expensive
 * scorer tree construction for clauses that match nothing on the segment.
 */
public class TestZeroCostScorerFix extends LuceneTestCase {

  private int savedMaxClauseCount;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    savedMaxClauseCount = IndexSearcher.getMaxClauseCount();
    IndexSearcher.setMaxClauseCount(Integer.MAX_VALUE);
  }

  @Override
  public void tearDown() throws Exception {
    IndexSearcher.setMaxClauseCount(savedMaxClauseCount);
    super.tearDown();
  }

  /**
   * Pure disjunction: 5 matching + 895 non-matching SHOULD clauses.
   * Measures scorer construction time directly on the disjunction's
   * BooleanScorerSupplier, isolating the opt() code path.
   *
   * With the fix: opt() skips 895 zero-cost clauses, only builds scorers for 5.
   * Without the fix: opt() builds scorers for all 900 clauses.
   */
  public void testPureDisjunctionScorerConstruction() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    // Index 1000 docs with known field values
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new StringField("field", "val_" + (i % 10), Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    int numMatching = 5;
    int numGhost = 895;

    // Build a pure disjunction: 5 matching + 895 ghost SHOULD clauses
    BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    for (int i = 0; i < numMatching; i++) {
      bqBuilder.add(new TermQuery(new Term("field", "val_" + i)), Occur.SHOULD);
    }
    for (int i = 0; i < numGhost; i++) {
      bqBuilder.add(new TermQuery(new Term("field", "ghost_" + i)), Occur.SHOULD);
    }

    BooleanQuery query = bqBuilder.build();
    Query rewritten = searcher.rewrite(query);

    LeafReaderContext leafCtx = reader.leaves().get(0);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
    // Measure scorerSupplier creation (iterates all 900 clauses, does term dict lookups)
    long supplierStartNanos = System.nanoTime();
    ScorerSupplier ss = weight.scorerSupplier(leafCtx);
    long supplierUs = (System.nanoTime() - supplierStartNanos) / 1000;
    assertNotNull(ss);

    // Measure scorer construction (this is where opt() runs)
    long startNanos = System.nanoTime();
    Scorer scorer = ss.get(ss.cost());
    long constructionUs = (System.nanoTime() - startNanos) / 1000;

    System.out.println("=== Pure Disjunction: " + numMatching + " matching + " + numGhost + " ghost ===");
    System.out.println("ScorerSupplier creation time (term dict lookups): " + supplierUs + " us");
    System.out.println("ScorerSupplier cost: " + ss.cost());
    System.out.println("Scorer construction time (opt()): " + constructionUs + " us");

    // Now build the same query with ONLY the 5 matching clauses (baseline)
    BooleanQuery.Builder cleanBuilder = new BooleanQuery.Builder();
    for (int i = 0; i < numMatching; i++) {
      cleanBuilder.add(new TermQuery(new Term("field", "val_" + i)), Occur.SHOULD);
    }
    BooleanQuery cleanQuery = cleanBuilder.build();
    Query cleanRewritten = searcher.rewrite(cleanQuery);
    Weight cleanWeight = searcher.createWeight(cleanRewritten, ScoreMode.COMPLETE, 1.0f);
    ScorerSupplier cleanSs = cleanWeight.scorerSupplier(leafCtx);
    assertNotNull(cleanSs);

    long cleanStart = System.nanoTime();
    Scorer cleanScorer = cleanSs.get(cleanSs.cost());
    long cleanConstructionUs = (System.nanoTime() - cleanStart) / 1000;

    System.out.println("Clean query (5 clauses only) construction: " + cleanConstructionUs + " us");

    // Verify correctness: both should return same results
    TopDocs topDocs = searcher.search(rewritten, 10);
    TopDocs cleanTopDocs = searcher.search(cleanRewritten, 10);
    System.out.println("900-clause hits: " + topDocs.totalHits.value());
    System.out.println("5-clause hits: " + cleanTopDocs.totalHits.value());
    assertEquals(topDocs.totalHits.value(), cleanTopDocs.totalHits.value());

    if (cleanConstructionUs > 0) {
      double ratio = (double) constructionUs / cleanConstructionUs;
      System.out.println("Slowdown ratio (900 vs 5 clauses): " + String.format("%.1fx", ratio));
      // With the fix, the ratio should be close to 1x since we skip ghost clauses.
      // Without the fix, it would be ~100x+ because we build 900 scorers.
    }

    reader.close();
    dir.close();
  }

  /**
   * Nested structure matching the issue exactly:
   * outer bool { MUST: term, FILTER: bool { SHOULD: [900 inner bools] } }
   * Measures full search time including scorer construction.
   */
  public void testNestedBoolWithManyGhostClauses() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new StringField("field1", "value1", Field.Store.NO));
      doc.add(new StringField("field2", "val_" + (i % 10), Field.Store.NO));
      doc.add(new StringField("field3", "code_" + (i % 5), Field.Store.NO));
      doc.add(new StringField("field4", "type_x", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    int numMatching = 5;
    int numGhost = 895;

    BooleanQuery.Builder shouldBuilder = new BooleanQuery.Builder();
    for (int i = 0; i < numMatching; i++) {
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(new TermQuery(new Term("field2", "val_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field3", "code_" + (i % 5))), Occur.FILTER);
      inner.add(new TermQuery(new Term("field4", "type_x")), Occur.FILTER);
      shouldBuilder.add(inner.build(), Occur.SHOULD);
    }
    for (int i = numMatching; i < numMatching + numGhost; i++) {
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(new TermQuery(new Term("field2", "ghost_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field3", "phantom_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field4", "missing_" + i)), Occur.FILTER);
      shouldBuilder.add(inner.build(), Occur.SHOULD);
    }

    BooleanQuery outerQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "value1")), Occur.MUST)
            .add(shouldBuilder.build(), Occur.FILTER)
            .build();

    // Warm up
    searcher.search(searcher.rewrite(outerQuery), 10);

    // Measure
    Query rewritten = searcher.rewrite(outerQuery);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
    LeafReaderContext leafCtx = reader.leaves().get(0);

    long supplierStart = System.nanoTime();
    ScorerSupplier ss = weight.scorerSupplier(leafCtx);
    long supplierUs = (System.nanoTime() - supplierStart) / 1000;

    long scorerStart = System.nanoTime();
    Scorer scorer = ss.get(ss.cost());
    long scorerUs = (System.nanoTime() - scorerStart) / 1000;

    long searchStart = System.nanoTime();
    TopDocs topDocs = searcher.search(rewritten, 10);
    long searchUs = (System.nanoTime() - searchStart) / 1000;

    System.out.println("\n=== Nested Bool: " + numMatching + " matching + " + numGhost + " ghost inner bools ===");
    System.out.println("ScorerSupplier creation time: " + supplierUs + " us");
    System.out.println("Scorer construction time (opt()): " + scorerUs + " us");
    System.out.println("Full search time: " + searchUs + " us");
    System.out.println("Total hits: " + topDocs.totalHits.value());
    assertTrue(topDocs.totalHits.value() > 0);

    // Baseline: same query with only matching clauses
    BooleanQuery.Builder cleanShouldBuilder = new BooleanQuery.Builder();
    for (int i = 0; i < numMatching; i++) {
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(new TermQuery(new Term("field2", "val_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field3", "code_" + (i % 5))), Occur.FILTER);
      inner.add(new TermQuery(new Term("field4", "type_x")), Occur.FILTER);
      cleanShouldBuilder.add(inner.build(), Occur.SHOULD);
    }
    BooleanQuery cleanOuter =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "value1")), Occur.MUST)
            .add(cleanShouldBuilder.build(), Occur.FILTER)
            .build();

    Query cleanRewritten = searcher.rewrite(cleanOuter);
    long cleanStart = System.nanoTime();
    TopDocs cleanTopDocs = searcher.search(cleanRewritten, 10);
    long cleanSearchUs = (System.nanoTime() - cleanStart) / 1000;

    System.out.println("Clean search time (5 clauses): " + cleanSearchUs + " us");
    System.out.println("Clean hits: " + cleanTopDocs.totalHits.value());
    assertEquals(topDocs.totalHits.value(), cleanTopDocs.totalHits.value());

    if (cleanSearchUs > 0) {
      System.out.println("Slowdown: " + String.format("%.1fx", (double) searchUs / cleanSearchUs));
    }

    reader.close();
    dir.close();
  }
}
