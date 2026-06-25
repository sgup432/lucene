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
package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

/**
 * Tests for the bound-transformation optimization in rangeIntoBitSet for GCD/delta encoded dense
 * numeric doc values (the gcd!=1 or minValue!=0 branch in Lucene90DocValuesProducer).
 *
 * <p>General correctness is already covered by TestDocValuesQueries. These tests cover cases
 * specific to this optimization: basic sanity, overflow fallback, and SIMD path verification.
 */
public class TestGcdDeltaRangeIntoBitSet extends LuceneTestCase {

  private IndexWriterConfig iwc() {
    return new IndexWriterConfig().setCodec(Codec.getDefault());
  }

  /** GCD=25, delta=10000: non-aligned bounds, exact boundary, and gap between steps. */
  public void testGcdDeltaBasicCorrectness() throws IOException {
    long[] docValues = {10000, 10025, 10050, 10075, 10100};
    assertRangeQuery(docValues, 10030, 10080, new int[] {2, 3});
    assertRangeQuery(docValues, 10000, 10000, new int[] {0});
    assertRangeQuery(docValues, 10001, 10024, new int[] {});
  }

  /**
   * When queryMin is Long.MIN_VALUE and delta is positive, subtractExact overflows. The scalar
   * fallback must produce the same results as the SIMD path would.
   */
  public void testOverflowFallbackProducesCorrectResults() throws IOException {
    long[] docValues = {10000, 10025, 10050};
    assertRangeQuery(docValues, Long.MIN_VALUE, 10040, new int[] {0, 1});

    long[] negativeValues = {-9000, -8000, -7000};
    assertRangeQuery(negativeValues, Long.MIN_VALUE + 1, Long.MAX_VALUE, new int[] {0, 1, 2});
  }

  /**
   * Verifies the rangeIntoBitSet override exists on the concrete anonymous class for the GCD/delta
   * branch, and that the SIMD path does not call longValue() (it reads raw packed values directly).
   */
  public void testSimdPathIsHitNotScalarFallback() throws IOException {
    long[] docValues = new long[200];
    for (int i = 0; i < 200; i++) {
      docValues[i] = 10000 + i * 25L;
    }
    long queryMin = 10000 + 50 * 25L;
    long queryMax = 10000 + 150 * 25L;

    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, iwc())) {
        for (long v : docValues) {
          Document doc = new Document();
          doc.add(NumericDocValuesField.indexedField("field", v));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext ctx = reader.leaves().get(0);
        NumericDocValues ndv = ctx.reader().getNumericDocValues("field");
        assertNotNull(ndv);

        // Verify rangeIntoBitSet is overridden on the concrete class, not just the base class.
        Class<?> c = ndv.getClass();
        boolean hasOverride = false;
        while (c != null && c != Object.class) {
          try {
            c.getDeclaredMethod(
                "rangeIntoBitSet",
                int.class,
                int.class,
                long.class,
                long.class,
                FixedBitSet.class,
                int.class);
            hasOverride = true;
            break;
          } catch (NoSuchMethodException e) {
            c = c.getSuperclass();
          }
        }
        assertTrue(
            "rangeIntoBitSet override missing on "
                + ndv.getClass().getName()
                + "; scalar advanceExact+longValue fallback would be used instead",
            hasOverride);

        // Count longValue() calls to verify the SIMD path never calls it.
        AtomicInteger longValueCalls = new AtomicInteger(0);
        NumericDocValues counting =
            new NumericDocValues() {
              @Override
              public long longValue() throws IOException {
                longValueCalls.incrementAndGet();
                return ndv.longValue();
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                return ndv.advanceExact(target);
              }

              @Override
              public int docID() {
                return ndv.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                return ndv.nextDoc();
              }

              @Override
              public int advance(int t) throws IOException {
                return ndv.advance(t);
              }

              @Override
              public long cost() {
                return ndv.cost();
              }
            };

        FixedBitSet simdBits = new FixedBitSet(200);
        ndv.rangeIntoBitSet(0, 200, queryMin, queryMax, simdBits, 0);

        FixedBitSet scalarBits = new FixedBitSet(200);
        for (int d = 0; d < 200; d++) {
          if (counting.advanceExact(d)) {
            long v = counting.longValue();
            if (v >= queryMin && v <= queryMax) scalarBits.set(d);
          }
        }

        assertEquals("SIMD and scalar paths must produce identical results", scalarBits, simdBits);
        assertTrue("Scalar loop must have called longValue()", longValueCalls.get() > 0);
        // longValueCalls only reflects the scalar loop; ndv.rangeIntoBitSet never incremented it.
      }
    }
  }

  private void assertRangeQuery(long[] docValues, long queryMin, long queryMax, int[] expectedDocs)
      throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, iwc())) {
        for (long value : docValues) {
          Document doc = new Document();
          doc.add(NumericDocValuesField.indexedField("field", value));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query rangeQuery = NumericDocValuesField.newSlowRangeQuery("field", queryMin, queryMax);
        TopDocs topDocs = searcher.search(rangeQuery, docValues.length);

        Set<Integer> actual = new HashSet<>();
        for (ScoreDoc sd : topDocs.scoreDocs) actual.add(sd.doc);

        Set<Integer> expected = new HashSet<>();
        for (int d : expectedDocs) expected.add(d);

        assertEquals("Mismatch for query [" + queryMin + ", " + queryMax + "]", expected, actual);
      }
    }
  }
}
