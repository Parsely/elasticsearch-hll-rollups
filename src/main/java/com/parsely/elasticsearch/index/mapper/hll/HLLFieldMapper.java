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

import com.carrotsearch.hppc.BitMixer;
import com.parsely.elasticsearch.search.aggregations.hll.HyperLogLogPlusPlus;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HLLFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    public static final String CONTENT_TYPE = "hll";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new HLLFieldType();
        // TODO: Change this to "precision threshold" to match current cardinality agg.
        public static final int PRECISION = HyperLogLogPlusPlus.DEFAULT_PRECISION;

        static {
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, HLLFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public HLLFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new HLLFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }

        public Builder precision(int precision) {
            ((HLLFieldType) fieldType).setPrecision(precision);
            return this;
        }


        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType.setIndexOptions(IndexOptions.NONE);
            defaultFieldType.setIndexOptions(IndexOptions.NONE);
            fieldType.setHasDocValues(true);
            defaultFieldType.setHasDocValues(true);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node,
                                          ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                // tweaking some settings is no longer allowed, the entire purpose of HLL fields is to store the whole HLL.
                if (propName.equals("doc_values")) {
                    throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
                } else if (propName.equals("index")) {
                    throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
                } else if (propName.equals("precision")) {
                    int precision = objectToInt(propNode);
                    if (precision < HyperLogLogPlusPlus.MIN_PRECISION) {
                        throw new MapperParsingException("Setting [precision] must be greater than " + HyperLogLogPlusPlus.MIN_PRECISION + " for field [" + name + "]");
                    } else if (precision > HyperLogLogPlusPlus.MAX_PRECISION) {
                        throw new MapperParsingException("Setting [precision] must be less than " + HyperLogLogPlusPlus.MAX_PRECISION + " for field [" + name + "]");
                    }
                    builder.precision(precision);
                    iterator.remove();
                }
            }
            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    public static class HLLFieldType extends MappedFieldType {

        private int precision = Defaults.PRECISION;

        public HLLFieldType() {
        }

        protected HLLFieldType(HLLFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public void checkCompatibility(MappedFieldType other, List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            if (precision != ((HLLFieldType) other).getPrecision()) {
                conflicts.add("mapper [" + name() + "] has different [precision] values");
            }
        }

        @Override
        public HLLFieldType clone() {
            return new HLLFieldType(this);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new BytesBinaryDVIndexFieldData.Builder();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "HLL fields are not searchable: [" + name() + "]");
        }

        public void setPrecision(int precision) {
            checkIfFrozen();
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }
    }

    private HLLFieldMapper(
            String simpleName,
            MappedFieldType fieldType,
            MappedFieldType defaultFieldType,
            Settings indexSettings,
            MultiFields multiFields,
            CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    public HLLFieldType fieldType() {
        return (HLLFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields)
            throws IOException {

        final List<Object> values;

        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            values = context.parser().list();
        } else {
            values = new ArrayList<>();
            values.add(context.parser().textOrNull());
        }
        if (values == null) {
            return;
        }

        // TODO: Check if this will be an LC HLL. If so, can we serialize the array of hashes directly? Instantiating this is expensive.
        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(fieldType().getPrecision(), BigArrays.NON_RECYCLING_INSTANCE, 0);


        // Add all collected values to the HLL
        for (Object value : values) {
            final BytesRef valueBytes = new BytesRef(value.toString());
            final long valueHash = MurmurHash3.hash128(valueBytes.bytes,
                    valueBytes.offset, valueBytes.length, 0, new MurmurHash3.Hash128()).h1;
            final long valueHashMixed = BitMixer.mix64(valueHash);
            counts.collect(0, valueHashMixed);
        }

        // Write the HLL to an OutputStream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos);

        counts.writeTo(0, osso);
        osso.close();


        // Convert the OutputStream to a byte array and save as field(s).
        byte[] hllBytes = baos.toByteArray();
        assert hllBytes.length != 0 : "Encoded HLL had no bytes";

        BytesRef hllBytesRef = new BytesRef(hllBytes);

        // stored as binary DocValues field

        fields.add(new BinaryDocValuesField(fieldType().name(), hllBytesRef));
        // also stored in field storage (optionally, for reindexing?)
        if (fieldType().stored()) {
            hllBytesRef.offset = 0;
            // TODO: is the .clone() the right move here?
            fields.add(new StoredField(name(), hllBytesRef.clone()));
        }


    }

    /**
     * Converts an Object to an int by checking it against known types first
     */
    private static int objectToInt(Object value) {
        int intValue;

        if (value instanceof Number) {
            intValue = ((Number) value).intValue();
        } else if (value instanceof BytesRef) {
            intValue = Integer.parseInt(((BytesRef) value).utf8ToString());
        } else {
            intValue = Integer.parseInt(value.toString());
        }

        return intValue;
    }
}