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

package com.parsely.elasticsearch.plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.parsely.elasticsearch.index.mapper.hll.HLLFieldMapper;

import com.parsely.elasticsearch.search.aggregations.hll.CardinalityAggregationBuilder;
import com.parsely.elasticsearch.search.aggregations.hll.CardinalityParser;
import com.parsely.elasticsearch.search.aggregations.hll.InternalCardinality;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import static java.util.Collections.singletonList;

public class HLLRollupPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(HLLFieldMapper.CONTENT_TYPE, new HLLFieldMapper.TypeParser());
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return singletonList(new AggregationSpec(CardinalityAggregationBuilder.NAME, CardinalityAggregationBuilder::new,
                new CardinalityParser()).addResultReader(InternalCardinality::new));
    }
}