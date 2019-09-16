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

package com.parsely.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;
import org.elasticsearch.common.util.BigArrays;
import com.parsely.elasticsearch.search.aggregations.hll.HyperLogLogPlusPlus;
import org.elasticsearch.test.ESTestCase;

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

import static com.parsely.elasticsearch.search.aggregations.hll.HyperLogLogPlusPlus.MAX_PRECISION;
import static org.hamcrest.Matchers.closeTo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.logging.log4j.Level.INFO;


public class HyperLogLogPlusPlusTests extends ESTestCase {

    private HyperLogLogPlusPlus makeRandomSketch(int size) {
        // utility to make a random HLL of a given size
        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(MAX_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 0);
        for (int i = 0; i < size; i++) {
            final int n = randomInt(100_000_000);
            final long hash = BitMixer.mix64(n);
            counts.collect(0, hash);
        }
        return counts;

    }

    public void testReadWrite() {
        // We'll test reading and writing HLL bytes for different cardinality sizes
        // ranging from 1 to 10 million. Beyond 10M, it takes awhile to generate the
        // random data.
        int[] sizes = new int[] {
                1,
                10,
                100,
                1_000,
                10_000,
                100_000,
                1_000_000, // 1 million
                10_000_000 // 10 million
        };

        for (int size : sizes) {
            // Our goal with the `baos` and `osso` is to just turn the `HyperLogLogPlusPlus.writeTo(...)`
            // call into a byte[]
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);
            HyperLogLogPlusPlus counts = makeRandomSketch(size);
            long cardinality = counts.cardinality(0);
            long precision = counts.precision();
            long maxBucket = counts.maxBucket();
            logger.printf(INFO, "%25s: [%d]", "Initial size of", size);
            logger.printf(INFO,"%25s: [%d]", "Initial cardinality of", cardinality);
            try {
                counts.writeTo(0, osso);
            } catch (IOException e) {
                fail();
            }
            // now we have the HLL as a byte[]
            byte[] hllBytes = baos.toByteArray();
            logger.printf(INFO,"%25s: [%d]", "Storing bytes on-disk", hllBytes.length);
            logger.printf(INFO,"---");
            // Our goal with `bais` and `issi` is to deserialize the byte[] into the HLL using
            // the `HyperLogLogPlusPlus.readFrom(...)` method
            ByteArrayInputStream bais = new ByteArrayInputStream(hllBytes);
            InputStreamStreamInput issi = new InputStreamStreamInput(bais);
            try {
                counts = HyperLogLogPlusPlus.readFrom(issi, BigArrays.NON_RECYCLING_INSTANCE);
            } catch (IOException e) {
                fail();
            }
            // now `counts` is our deserialized HLL; this assertion
            // confirms it by comparing their values
            assertEquals(cardinality, counts.cardinality(0));
            assertEquals(precision, counts.precision());
            assertEquals(maxBucket, counts.maxBucket());
        }
    }
}