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

package com.parsely.elasticsearch.index.mapper.hll;

import com.parsely.elasticsearch.search.aggregations.hll.HyperLogLogPlusPlus;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class HLLFieldMapperTests extends ESSingleNodeTestCase {

    MapperRegistry mapperRegistry;
    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        mapperRegistry = new MapperRegistry(
                Collections.singletonMap(HLLFieldMapper.CONTENT_TYPE, new HLLFieldMapper.TypeParser()),
                Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        Supplier<QueryShardContext> queryShardContext = () -> {
            return indexService.newQueryShardContext(0, null, () -> { throw new UnsupportedOperationException(); }, null);
        };
        parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(), indexService.getIndexAnalyzers(),
                indexService.xContentRegistry(), indexService.similarityService(), mapperRegistry, queryShardContext);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    private String getTestMappingString() throws IOException {
        return Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("test-doc-type")
                      .startObject("properties").startObject("test-hll-field")
                      .field("type", "hll")
                      .endObject().endObject().endObject().endObject());
    }

    private String getTestMappingString(long precision) throws IOException {
        return Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("test-doc-type")
                .startObject("properties").startObject("test-hll-field")
                .field("type", "hll")
                .field("precision", precision)
                .endObject().endObject().endObject().endObject());
    }

    public void testDefaults() throws Exception {
        // Create the mapper and verify the default value for precision
        DocumentMapper mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString()));
        HLLFieldMapper hllMapper = (HLLFieldMapper) mapper.mappers().getMapper("test-hll-field");
        assertEquals(HLLFieldMapper.Defaults.PRECISION, hllMapper.fieldType().getPrecision());

        // Parse a test document and validate output
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "test-doc-type", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("test-hll-field", "foo")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("test-hll-field");
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.BINARY, field.fieldType().docValuesType());
        // TODO: Is it worth checking the bytes here so we know if the underlying HLL implementation changes?
        assertEquals(7, field.binaryValue().length);
    }

    public void testChangingPrecision() throws Exception {
        int[] numbers = IntStream.range(0, 10000).toArray();
        BytesReference docBytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("test-hll-field", numbers)
                .endObject());

        // Create the mapper and verify precision is 18
        DocumentMapper mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString(18)));
        HLLFieldMapper hllMapper = (HLLFieldMapper) mapper.mappers().getMapper("test-hll-field");
        assertEquals(18, hllMapper.fieldType().getPrecision());

        // Parse the test document and validate output bytes length
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "test-doc-type", "1", docBytes, XContentType.JSON));
        IndexableField field = parsedDoc.rootDoc().getFields("test-hll-field")[0];
        assertEquals(40000, field.binaryValue().length);

        // Create the mapper and verify precision is now 14
        mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString(10)));
        hllMapper = (HLLFieldMapper) mapper.mappers().getMapper("test-hll-field");
        assertEquals(10, hllMapper.fieldType().getPrecision());

        // Parse a test document and validate output
        parsedDoc = mapper.parse(SourceToParse.source("test", "test-doc-type", "1", docBytes, XContentType.JSON));
        field = parsedDoc.rootDoc().getFields("test-hll-field")[0];
        assertEquals(1026, field.binaryValue().length);
    }

    public void testPrecisionTooLow() throws Exception {
        // Create mapper where precision is one too low.
        int precision = HyperLogLogPlusPlus.MIN_PRECISION - 1;
        try {
            DocumentMapper mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString(precision)));
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [precision] must be greater than"));
        }
    }

    public void testPrecisionTooHigh() throws Exception {
        // Create mapper where precision is one too high.
        int precision = HyperLogLogPlusPlus.MAX_PRECISION + 1;
        try {
            DocumentMapper mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString(precision)));
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [precision] must be less than"));
        }
    }

    public void testDocValuesSettingNotAllowed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "hll")
                .field("doc_values", false)
                .endObject().endObject().endObject().endObject());
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }

        // Even setting to the default is not allowed, the setting is invalid
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "hll")
                .field("doc_values", true)
                .endObject().endObject().endObject().endObject());
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }
    }

    public void testIndexSettingNotAllowed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "hll")
                .field("index", "not_analyzed")
                .endObject().endObject().endObject().endObject());
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }

        // even setting to the default is not allowed, the setting is invalid
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "hll")
                .field("index", "no")
                .endObject().endObject().endObject().endObject());
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }
    }

    public void testEmptyName() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("")
                .field("type", "hll")
                .endObject().endObject().endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testArrayOfItems() throws Exception {
        DocumentMapper mapper = parser.parse("test-doc-type", new CompressedXContent(getTestMappingString()));
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "test-doc-type", "1", BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("test-hll-field",  new String[] {"foo", "bar", "baz"})
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("test-hll-field");
        IndexableField field = fields[0];
        // TODO: Is it worth checking the bytes here so we know if the underlying HLL implementation changes?
        assertEquals(15, field.binaryValue().length);
    }
}