/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.parsely.elasticsearch.search.aggregations.hll;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource valuesSource;

    // Can't initialize this until we know what precision we need, and that's determined by the data in the index.
    @Nullable
    private HyperLogLogPlusPlus counts;

    private Collector collector;

    public CardinalityAggregator(String name, ValuesSource valuesSource,
                                 SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.counts = null;
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }
        // FIXME: Do we need to guard around this to ensure it's always ValuesSource.Bytes?
        ValuesSource.Bytes source = (ValuesSource.Bytes) valuesSource;
        SortedBinaryDocValues rollupValues = source.bytesValues(ctx);

        return new RollupCollector(this, rollupValues);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() throws IOException {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        HyperLogLogPlusPlus copy = new HyperLogLogPlusPlus(counts.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalCardinality(name, copy, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect() throws IOException;

    }

    private static class EmptyCollector extends Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // FIXME: Clean up the rest of this class since the other Collectors are gone.
    private static class RollupCollector extends Collector {

        private final CardinalityAggregator parentAggregator;
        private final SortedBinaryDocValues rollups;

        RollupCollector(CardinalityAggregator parentAggregator, SortedBinaryDocValues rollups) {
            this.parentAggregator = parentAggregator;
            this.rollups = rollups;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            // Each binary blob is in the SortedBinaryDocValues object, so we just advance along deserialize, and merge.
            if (rollups.advanceExact(doc)) {
                BytesRef bytes = rollups.nextValue();
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes.bytes);
                InputStreamStreamInput issi = new InputStreamStreamInput(bais);
                if (parentAggregator.counts == null) {
                    parentAggregator.counts = HyperLogLogPlusPlus.readFrom(issi, BigArrays.NON_RECYCLING_INSTANCE);
                } else {
                    // Using mergeFrom
                    parentAggregator.counts.mergeFrom(bucketOrd, issi);
                }
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

    }
}