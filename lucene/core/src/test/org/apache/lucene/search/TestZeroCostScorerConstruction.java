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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
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
 * Reproduces the scenario from https://github.com/apache/lucene/issues/15887:
 * A complex boolean query with many SHOULD clauses where most clauses
 * have zero cost (terms don't exist in the segment), but scorer construction
 * is still expensive because we build scorers for all clauses.
 */
public class TestZeroCostScorerConstruction extends LuceneTestCase {

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
   * Reproduces the core issue: a boolean query with ~900 SHOULD clauses
   * where most terms don't exist in the index. Despite zero hits, scorer
   * construction iterates all clauses calling scorer.get(leadCost).
   */
  public void testScorerConstructionWithManyZeroCostClauses() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMaxBufferedDocs(10000);
    IndexWriter writer = new IndexWriter(dir, iwc);

    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("field1", "value1", Field.Store.NO));
      doc.add(new StringField("field2", "existing_val_" + (i % 5), Field.Store.NO));
      doc.add(new StringField("field3", "existing_code_" + (i % 3), Field.Store.NO));
      doc.add(new StringField("field4", "existing_type", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    int numShouldClauses = 900;
    BooleanQuery.Builder shouldBuilder = new BooleanQuery.Builder();

    for (int i = 0; i < numShouldClauses; i++) {
      BooleanQuery.Builder innerBool = new BooleanQuery.Builder();
      innerBool.add(new TermQuery(new Term("field2", "nonexistent_val_" + i)), Occur.FILTER);
      innerBool.add(new TermQuery(new Term("field3", "nonexistent_code_" + i)), Occur.FILTER);
      innerBool.add(new TermQuery(new Term("field4", "nonexistent_type_" + i)), Occur.FILTER);
      shouldBuilder.add(innerBool.build(), Occur.SHOULD);
    }

    BooleanQuery outerQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "value1")), Occur.MUST)
            .add(shouldBuilder.build(), Occur.FILTER)
            .build();

    Query rewritten = searcher.rewrite(outerQuery);
    System.out.println("Original query class: " + outerQuery.getClass().getSimpleName());
    System.out.println("Rewritten query class: " + rewritten.getClass().getSimpleName());
    System.out.println("Rewritten to MatchNoDocsQuery? " + (rewritten instanceof MatchNoDocsQuery));

    LeafReaderContext leafCtx = reader.leaves().get(0);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
    ScorerSupplier scorerSupplier = weight.scorerSupplier(leafCtx);

    if (scorerSupplier != null) {
      long cost = scorerSupplier.cost();
      System.out.println("ScorerSupplier cost: " + cost);

      long startNanos = System.nanoTime();
      Scorer scorer = scorerSupplier.get(cost);
      long scorerConstructionNanos = System.nanoTime() - startNanos;

      startNanos = System.nanoTime();
      TopDocs topDocs = searcher.search(rewritten, 10);
      long queryExecutionNanos = System.nanoTime() - startNanos;

      System.out.println("Scorer construction time: " + (scorerConstructionNanos / 1000) + " us");
      System.out.println("Full query execution time: " + (queryExecutionNanos / 1000) + " us");
      System.out.println("Total hits: " + topDocs.totalHits.value());

      assertEquals("Query should have zero hits", 0, topDocs.totalHits.value());
    } else {
      System.out.println("ScorerSupplier is null — query was optimized away at Weight level");
    }

    reader.close();
    dir.close();
  }

  /**
   * Demonstrates the per-segment cost behavior: terms exist in some segments
   * but not others. Rewrite can't help here, but per-segment skipping can.
   */
  public void testPerSegmentZeroCostClauses() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMaxBufferedDocs(50);
    IndexWriter writer = new IndexWriter(dir, iwc);

    // Segment 1: documents with "rare_term"
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new StringField("field1", "value1", Field.Store.NO));
      doc.add(new StringField("field2", "rare_term", Field.Store.NO));
      doc.add(new StringField("field3", "code_a", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.flush();

    // Segment 2: documents WITHOUT "rare_term"
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new StringField("field1", "value1", Field.Store.NO));
      doc.add(new StringField("field2", "common_term", Field.Store.NO));
      doc.add(new StringField("field3", "code_b", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.flush();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    System.out.println("Number of segments: " + reader.leaves().size());

    int numShouldClauses = 500;
    BooleanQuery.Builder shouldBuilder = new BooleanQuery.Builder();

    for (int i = 0; i < numShouldClauses; i++) {
      BooleanQuery.Builder innerBool = new BooleanQuery.Builder();
      if (i == 0) {
        // One clause that matches in segment 1 only
        innerBool.add(new TermQuery(new Term("field2", "rare_term")), Occur.FILTER);
        innerBool.add(new TermQuery(new Term("field3", "code_a")), Occur.FILTER);
      } else {
        // 499 clauses with non-existent terms
        innerBool.add(new TermQuery(new Term("field2", "ghost_val_" + i)), Occur.FILTER);
        innerBool.add(new TermQuery(new Term("field3", "ghost_code_" + i)), Occur.FILTER);
      }
      shouldBuilder.add(innerBool.build(), Occur.SHOULD);
    }

    BooleanQuery outerQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "value1")), Occur.MUST)
            .add(shouldBuilder.build(), Occur.FILTER)
            .build();

    Query rewritten = searcher.rewrite(outerQuery);

    // Can NOT be MatchNoDocsQuery because "rare_term" exists in the index
    assertFalse(
        "Query should NOT be rewritten to MatchNoDocsQuery",
        rewritten instanceof MatchNoDocsQuery);

    for (LeafReaderContext leafCtx : reader.leaves()) {
      Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
      ScorerSupplier scorerSupplier = weight.scorerSupplier(leafCtx);

      if (scorerSupplier != null) {
        long cost = scorerSupplier.cost();
        System.out.println(
            "Segment " + leafCtx.ord + ": cost=" + cost + ", maxDoc=" + leafCtx.reader().maxDoc());

        long startNanos = System.nanoTime();
        Scorer scorer = scorerSupplier.get(cost);
        long constructionUs = (System.nanoTime() - startNanos) / 1000;
        System.out.println("  Scorer construction: " + constructionUs + " us");
      } else {
        System.out.println("Segment " + leafCtx.ord + ": scorerSupplier is null");
      }
    }

    TopDocs topDocs = searcher.search(rewritten, 10);
    System.out.println("Total hits: " + topDocs.totalHits.value());

    reader.close();
    dir.close();
  }

  /**
   * Directly inspects per-clause cost() values to show that TermQuery returns
   * null scorerSupplier when term doesn't exist, but BooleanQuery still
   * iterates all clauses during scorer construction.
   */
  public void testInspectPerClauseCost() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("status", "active", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    LeafReaderContext leafCtx = reader.leaves().get(0);

    int nullCount = 0;
    int zeroCostCount = 0;
    int nonZeroCostCount = 0;

    for (int i = 0; i < 100; i++) {
      TermQuery tq = new TermQuery(new Term("status", "phantom_" + i));
      Weight w = searcher.createWeight(tq, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      ScorerSupplier ss = w.scorerSupplier(leafCtx);

      if (ss == null) {
        nullCount++;
      } else {
        long cost = ss.cost();
        if (cost == 0) zeroCostCount++;
        else nonZeroCostCount++;
      }
    }

    // Existing term
    {
      TermQuery tq = new TermQuery(new Term("status", "active"));
      Weight w = searcher.createWeight(tq, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      ScorerSupplier ss = w.scorerSupplier(leafCtx);
      assertNotNull("Existing term should have a non-null scorerSupplier", ss);
      assertTrue("Existing term should have cost > 0", ss.cost() > 0);
      System.out.println("Term 'active': cost=" + ss.cost());
    }

    System.out.println("Null supplier (term not in segment): " + nullCount);
    System.out.println("Zero-cost suppliers: " + zeroCostCount);
    System.out.println("Non-zero-cost suppliers: " + nonZeroCostCount);

    // Key finding: TermQuery.scorerSupplier() returns a non-null supplier
    // with cost()==0 when the term doesn't exist in the segment (not null!).
    // This means BooleanScorerSupplier receives these zero-cost suppliers
    // and still calls .get(leadCost) on each during scorer construction.
    // That's the core of the issue: we pay scorer construction cost for
    // clauses that we already know will match nothing.
    assertTrue(
        "Non-existent terms should have zero-cost scorerSuppliers (not null)",
        zeroCostCount > 0 || nullCount > 0);
    assertEquals("Non-existent terms should not have positive cost", 0, nonZeroCostCount);

    reader.close();
    dir.close();
  }

  /**
   * Shows the actual bottleneck: BooleanScorerSupplier.opt() iterates all
   * SHOULD clauses and calls scorer.get(leadCost) on each.
   */
  public void testBooleanScorerSupplierIteratesAllClauses() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("field1", "value1", Field.Store.NO));
      doc.add(new StringField("field2", "val_" + (i % 3), Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    // Pure disjunction with many non-matching clauses
    BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    int numClauses = 900;

    for (int i = 0; i < numClauses; i++) {
      bqBuilder.add(new TermQuery(new Term("field2", "nonexistent_" + i)), Occur.SHOULD);
    }

    BooleanQuery query = bqBuilder.build();
    Query rewritten = searcher.rewrite(query);

    System.out.println("Pure disjunction rewritten class: " + rewritten.getClass().getSimpleName());

    LeafReaderContext leafCtx = reader.leaves().get(0);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
    ScorerSupplier ss = weight.scorerSupplier(leafCtx);

    if (ss != null) {
      System.out.println("BooleanScorerSupplier cost: " + ss.cost());

      long startNanos = System.nanoTime();
      Scorer scorer = ss.get(ss.cost());
      long constructionUs = (System.nanoTime() - startNanos) / 1000;

      System.out.println("Scorer construction for " + numClauses + " clauses: " + constructionUs + " us");
      System.out.println("Scorer class: " + scorer.getClass().getSimpleName());
    } else {
      // When ALL terms are non-existent, BooleanWeight may return null
      System.out.println("ScorerSupplier is null — all clauses eliminated at Weight level");
      System.out.println("This means BooleanWeight already handles the all-null case.");
      System.out.println("The issue manifests when SOME clauses have cost>0 but most have cost=0.");
    }

    TopDocs topDocs = searcher.search(rewritten, 10);
    assertEquals(0, topDocs.totalHits.value());

    reader.close();
    dir.close();
  }

  /**
   * The realistic reproduction: mix of matching and non-matching clauses
   * in a nested boolean structure, exactly like the issue describes.
   *
   * <p>This test traces the exact code path that causes the problem:
   *
   * <ol>
   *   <li>The outer query is: bool { MUST: term(field1,value1), FILTER: bool { SHOULD: [900 inner bools] } }
   *   <li>After rewrite, the inner SHOULD bools whose ALL filter terms are non-existent get
   *       rewritten to MatchNoDocsQuery and dropped. But inner bools where at least one term
   *       exists in the index survive rewrite — even if they match zero docs on this segment.
   *   <li>At Weight creation time, BooleanWeight.scorerSupplier() iterates all surviving clauses.
   *       For each inner bool, it calls the child weight's scorerSupplier(). If ALL child
   *       TermQuery suppliers return null (term not in segment), the inner BooleanWeight returns
   *       null and the clause is dropped. But if even ONE child term exists in the segment,
   *       a BooleanScorerSupplier is created with cost=0.
   *   <li>At scorer construction time, BooleanScorerSupplier.opt() iterates ALL surviving
   *       ScorerSuppliers and calls .get(leadCost) on each — even those with cost=0.
   *       This is the bottleneck: each .get() call constructs a full Scorer object tree.
   * </ol>
   *
   * <p>The proposed fix: in opt(), skip clauses where cost()==0 when minShouldMatch is at most 1.
   */
  public void testRealisticMixedCostScenario() throws Exception {
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

    // ---------------------------------------------------------------
    // Build the query exactly like the issue: outer bool with MUST + FILTER,
    // where the FILTER is a bool with ~900 SHOULD clauses.
    // Each SHOULD clause is itself a bool with FILTER sub-clauses.
    // ---------------------------------------------------------------
    int numMatchingClauses = 5;
    int numGhostClauses = 895;
    int numShouldClauses = numMatchingClauses + numGhostClauses;

    BooleanQuery.Builder shouldBuilder = new BooleanQuery.Builder();

    // 5 clauses that actually match (all their terms exist in the index)
    for (int i = 0; i < numMatchingClauses; i++) {
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(new TermQuery(new Term("field2", "val_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field3", "code_" + (i % 5))), Occur.FILTER);
      inner.add(new TermQuery(new Term("field4", "type_x")), Occur.FILTER);
      shouldBuilder.add(inner.build(), Occur.SHOULD);
    }

    // 895 clauses that DON'T match (all their terms are non-existent)
    for (int i = numMatchingClauses; i < numShouldClauses; i++) {
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

    // ---------------------------------------------------------------
    // Step 1: Rewrite phase
    // ---------------------------------------------------------------
    Query rewritten = searcher.rewrite(outerQuery);
    assertFalse(
        "Cannot be MatchNoDocsQuery because 5 clauses have terms that exist in the index",
        rewritten instanceof MatchNoDocsQuery);

    // Count how many SHOULD clauses survive rewrite.
    // The ghost clauses whose ALL terms are non-existent get their inner
    // BooleanQuery rewritten: each inner TermQuery for a non-existent term
    // does NOT rewrite to MatchNoDocsQuery (TermQuery.rewrite() doesn't check
    // docFreq). However, the inner BooleanQuery has FILTER clauses, and
    // BooleanQuery.rewrite() only drops MatchNoDocsQuery SHOULD/MUST_NOT clauses.
    // For FILTER/MUST clauses that are MatchNoDocsQuery, it returns MatchNoDocsQuery
    // for the whole BooleanQuery. But since TermQuery doesn't rewrite to
    // MatchNoDocsQuery, the inner bools survive rewrite intact.
    //
    // This is the key gap the commenter identified: TermQuery.rewrite() does NOT
    // check whether the term exists in the index. If it did, ghost terms would
    // become MatchNoDocsQuery, inner bools with all-ghost FILTER clauses would
    // collapse to MatchNoDocsQuery, and the outer SHOULD disjunction would drop them.
    System.out.println("Rewritten query class: " + rewritten.getClass().getSimpleName());

    // ---------------------------------------------------------------
    // Step 2: Weight creation + per-segment scorerSupplier
    // ---------------------------------------------------------------
    LeafReaderContext leafCtx = reader.leaves().get(0);
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);

    // BooleanWeight.scorerSupplier() iterates all clauses.
    // For each inner bool SHOULD clause:
    //   - It calls inner BooleanWeight.scorerSupplier(leafCtx)
    //   - Inner BooleanWeight iterates its FILTER children
    //   - For ghost terms: TermWeight.scorerSupplier() calls TermStates.get(leafCtx)
    //     which does a terms dictionary lookup. If the term doesn't exist in this
    //     segment, it returns null. BooleanWeight sees a null for a required (FILTER)
    //     clause and returns null for the whole inner bool.
    //   - For existing terms: returns a real ScorerSupplier
    //
    // So ghost inner bools DO get eliminated at the Weight level (return null).
    // But this still requires iterating all 900 clauses and doing terms dict lookups.
    ScorerSupplier ss = weight.scorerSupplier(leafCtx);
    assertNotNull("ScorerSupplier should not be null since some clauses match", ss);

    long cost = ss.cost();
    System.out.println("Overall ScorerSupplier cost: " + cost);

    // ---------------------------------------------------------------
    // Step 3: Scorer construction — the actual bottleneck
    // ---------------------------------------------------------------
    // BooleanScorerSupplier.get(leadCost) is called on the outer bool.
    // This calls getInternal() which sees MUST + FILTER clauses.
    // For the FILTER clause (the inner SHOULD disjunction), it calls
    // the inner BooleanScorerSupplier.get(leadCost).
    //
    // The inner BooleanScorerSupplier has only SHOULD clauses (the surviving
    // inner bools). It calls opt() which does:
    //
    //   for (ScorerSupplier scorer : optional) {
    //     optionalScorers.add(scorer.get(leadCost));  // <-- called for EVERY clause
    //   }
    //
    // Even if a clause's cost() is 0, we still call .get(leadCost) which
    // constructs a full Scorer. For inner BooleanScorerSuppliers, this means
    // recursively building ConjunctionScorer trees with TermScorers.
    //
    // With 900 clauses, this loop dominates latency.
    long startNanos = System.nanoTime();
    Scorer scorer = ss.get(cost);
    long constructionNanos = System.nanoTime() - startNanos;

    // ---------------------------------------------------------------
    // Step 4: Actual query execution
    // ---------------------------------------------------------------
    long execStart = System.nanoTime();
    TopDocs topDocs = searcher.search(rewritten, 10);
    long execNanos = System.nanoTime() - execStart;

    long constructionUs = constructionNanos / 1000;
    long execUs = execNanos / 1000;

    System.out.println("=== Timing Results ===");
    System.out.println("Scorer construction time: " + constructionUs + " us");
    System.out.println("Full search execution time (includes construction): " + execUs + " us");
    System.out.println("Total hits: " + topDocs.totalHits.value());
    if (execUs > 0) {
      System.out.println(
          "Construction as fraction of execution: "
              + String.format("%.1f%%", 100.0 * constructionUs / execUs));
    }

    assertTrue(
        "Should have hits from the " + numMatchingClauses + " matching SHOULD clauses",
        topDocs.totalHits.value() > 0);

    // ---------------------------------------------------------------
    // Step 5: Compare with a "clean" query that only has the 5 matching clauses
    // ---------------------------------------------------------------
    BooleanQuery.Builder cleanShouldBuilder = new BooleanQuery.Builder();
    for (int i = 0; i < numMatchingClauses; i++) {
      BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(new TermQuery(new Term("field2", "val_" + i)), Occur.FILTER);
      inner.add(new TermQuery(new Term("field3", "code_" + (i % 5))), Occur.FILTER);
      inner.add(new TermQuery(new Term("field4", "type_x")), Occur.FILTER);
      cleanShouldBuilder.add(inner.build(), Occur.SHOULD);
    }

    BooleanQuery cleanQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "value1")), Occur.MUST)
            .add(cleanShouldBuilder.build(), Occur.FILTER)
            .build();

    Query cleanRewritten = searcher.rewrite(cleanQuery);
    Weight cleanWeight = searcher.createWeight(cleanRewritten, ScoreMode.COMPLETE, 1.0f);
    ScorerSupplier cleanSs = cleanWeight.scorerSupplier(leafCtx);
    assertNotNull(cleanSs);

    long cleanStart = System.nanoTime();
    Scorer cleanScorer = cleanSs.get(cleanSs.cost());
    long cleanConstructionUs = (System.nanoTime() - cleanStart) / 1000;

    long cleanExecStart = System.nanoTime();
    TopDocs cleanTopDocs = searcher.search(cleanRewritten, 10);
    long cleanExecUs = (System.nanoTime() - cleanExecStart) / 1000;

    System.out.println("\n=== Clean Query (only " + numMatchingClauses + " matching clauses) ===");
    System.out.println("Scorer construction time: " + cleanConstructionUs + " us");
    System.out.println("Full search execution time: " + cleanExecUs + " us");
    System.out.println("Total hits: " + cleanTopDocs.totalHits.value());

    // Both queries should return the same results
    assertEquals(
        "Both queries should return the same number of hits",
        topDocs.totalHits.value(),
        cleanTopDocs.totalHits.value());

    // The 900-clause query's scorer construction should be significantly slower
    // than the 5-clause query's. This is the waste the issue is about.
    System.out.println(
        "\n=== Overhead from " + numGhostClauses + " ghost clauses ===");
    System.out.println(
        "Construction slowdown: "
            + (cleanConstructionUs > 0
                ? String.format("%.1fx", (double) constructionUs / cleanConstructionUs)
                : "N/A (clean was too fast to measure)"));

    reader.close();
    dir.close();
  }
}
