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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.FixedBitSet;

/**
 * Per-block value-sorted index for numeric doc values. For each skip block, stores (value, docID)
 * pairs sorted by value using fixed-width encoding for random access binary search.
 *
 * <p>Format:
 * <ul>
 *   <li>VInt: numBlocks
 *   <li>VInt: blockSize
 *   <li>For each block: VInt count (number of entries)
 *   <li>Then contiguous block data: for each block, count longs (sorted values) followed by
 *       count ints (corresponding docIDs)
 * </ul>
 *
 * @lucene.experimental
 */
public final class ValueSortedBlockIndex {

  private ValueSortedBlockIndex() {}

  /**
   * Write sorted (value, docID) pairs for a field, organized by skip blocks.
   * Values are stored as raw longs, docIDs as raw ints, for random access.
   */
  public static void writeField(
      IndexOutput output, SortedNumericDocValues values, int maxDoc, int blockSize)
      throws IOException {
    int numBlocks = (maxDoc + blockSize - 1) / blockSize;
    output.writeVInt(numBlocks);
    output.writeVInt(blockSize);

    long[] blockValues = new long[blockSize];
    int[] blockDocIDs = new int[blockSize];

    // First pass: collect and write block counts
    int doc = values.nextDoc();
    int[] counts = new int[numBlocks];
    long[][] allValues = new long[numBlocks][];
    int[][] allDocIDs = new int[numBlocks][];

    for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
      int blockEnd = Math.min((blockIdx + 1) * blockSize, maxDoc);
      int count = 0;

      while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < blockEnd) {
        long value = values.nextValue();
        if (count >= blockValues.length) {
          blockValues = Arrays.copyOf(blockValues, count * 2);
          blockDocIDs = Arrays.copyOf(blockDocIDs, count * 2);
        }
        blockValues[count] = value;
        blockDocIDs[count] = doc;
        count++;
        for (int i = 1; i < values.docValueCount(); i++) {
          values.nextValue();
        }
        doc = values.nextDoc();
      }

      // Sort by value
      long[] vals = Arrays.copyOf(blockValues, count);
      int[] docs = Arrays.copyOf(blockDocIDs, count);
      sortByValue(vals, docs, count);
      counts[blockIdx] = count;
      allValues[blockIdx] = vals;
      allDocIDs[blockIdx] = docs;

      // Write count
      output.writeVInt(count);
    }

    // Second pass: write block data (fixed-width for random access)
    for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
      int count = counts[blockIdx];
      long[] vals = allValues[blockIdx];
      int[] docs = allDocIDs[blockIdx];
      // Write sorted values as raw longs
      for (int i = 0; i < count; i++) {
        output.writeLong(vals[i]);
      }
      // Write docIDs as raw ints
      for (int i = 0; i < count; i++) {
        output.writeInt(docs[i]);
      }
    }
  }

  /** Sort (value, docID) pairs by value. Package-private for use by the consumer. */
  static void sortByValue(long[] values, int[] docIDs, int count) {
    if (count <= 64) {
      for (int i = 1; i < count; i++) {
        long val = values[i];
        int doc = docIDs[i];
        int j = i - 1;
        while (j >= 0 && values[j] > val) {
          values[j + 1] = values[j];
          docIDs[j + 1] = docIDs[j];
          j--;
        }
        values[j + 1] = val;
        docIDs[j + 1] = doc;
      }
    } else {
      Integer[] indices = new Integer[count];
      for (int i = 0; i < count; i++) indices[i] = i;
      Arrays.sort(indices, (a, b) -> Long.compare(values[a], values[b]));
      long[] sv = new long[count];
      int[] sd = new int[count];
      for (int i = 0; i < count; i++) {
        sv[i] = values[indices[i]];
        sd[i] = docIDs[indices[i]];
      }
      System.arraycopy(sv, 0, values, 0, count);
      System.arraycopy(sd, 0, docIDs, 0, count);
    }
  }

  /** Reader for the value-sorted block index with random-access binary search. */
  public static class Reader implements Closeable {
    private final int numBlocks;
    private final int blockSize;
    private final RandomAccessInput blockData;
    private final long blockDataStart;
    private final long[] cachedOffsets;
    private final int[] cachedCounts;
    private final int[] cachedMinDocIDs;

    public Reader(IndexInput input) throws IOException {
      this.numBlocks = input.readVInt();
      this.blockSize = input.readVInt();
      this.cachedOffsets = new long[numBlocks + 1];
      this.cachedCounts = new int[numBlocks];
      this.cachedMinDocIDs = new int[numBlocks];

      long offset = 0;
      for (int b = 0; b < numBlocks; b++) {
        cachedCounts[b] = input.readVInt();
        cachedMinDocIDs[b] = input.readVInt();
        cachedOffsets[b] = offset;
        offset += (long) cachedCounts[b] * 8 + (long) cachedCounts[b] * 4;
      }
      cachedOffsets[numBlocks] = offset;

      this.blockDataStart = input.getFilePointer();
      this.blockData = input.randomAccessSlice(blockDataStart, offset);
      input.seek(blockDataStart + offset);
    }

    /** Find the VSI block index that contains the given docID. Returns -1 if not found. */
    public int findBlockByDocID(int docID) {
      // Binary search on cachedMinDocIDs
      int lo = 0, hi = numBlocks - 1;
      int result = -1;
      while (lo <= hi) {
        int mid = (lo + hi) >>> 1;
        if (cachedMinDocIDs[mid] <= docID) {
          result = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      return result;
    }

    /**
     * Find all doc IDs in the given block whose values fall within [lower, upper]
     * and set them in the bitset. Returns the number of matching docs.
     */
    public int findDocsInRange(int blockIdx, long lower, long upper, FixedBitSet bitSet, int offset)
        throws IOException {
      if (blockIdx < 0 || blockIdx >= numBlocks) return 0;
      int count = cachedCounts[blockIdx];
      if (count == 0) return 0;

      long blockOffset = cachedOffsets[blockIdx];

      // Binary search for lower bound in sorted values
      int lo = lowerBound(blockOffset, count, lower);
      // Binary search for upper bound
      int hi = upperBound(blockOffset, count, upper);

      // Read matching docIDs and set bits
      long docIDsOffset = blockOffset + (long) count * 8;
      int matched = 0;
      for (int i = lo; i < hi; i++) {
        int docID = blockData.readInt(docIDsOffset + (long) i * 4);
        int bitIdx = docID - offset;
        if (bitIdx >= 0 && bitIdx < bitSet.length()) {
          bitSet.set(bitIdx);
          matched++;
        }
      }
      return matched;
    }

    /** Binary search: find first index where value >= target. */
    private int lowerBound(long blockOffset, int count, long target) throws IOException {
      int lo = 0, hi = count;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        long val = blockData.readLong(blockOffset + (long) mid * 8);
        if (val < target) lo = mid + 1;
        else hi = mid;
      }
      return lo;
    }

    /** Binary search: find first index where value > target. */
    private int upperBound(long blockOffset, int count, long target) throws IOException {
      int lo = 0, hi = count;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        long val = blockData.readLong(blockOffset + (long) mid * 8);
        if (val <= target) lo = mid + 1;
        else hi = mid;
      }
      return lo;
    }

    public int getBlockIndex(int docID) {
      return docID / blockSize;
    }

    public int getNumBlocks() {
      return numBlocks;
    }

    public int getBlockSize() {
      return blockSize;
    }

    @Override
    public void close() throws IOException {
      // blockData is owned by the caller's IndexInput
    }
  }
}
