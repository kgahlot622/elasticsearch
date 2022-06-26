/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;


/**
 * Mapper for the {@code _seq_no} field.
 *
 * We expect to use the seq# for sorting, during collision checking and for
 * doing range searches. Therefore the {@code _seq_no} field is stored both
 * as a numeric doc value and as numeric indexed field.
 *
 * This mapper also manages the primary term field, which has no ES named
 * equivalent. The primary term is only used during collision after receiving
 * identical seq# values for two document copies. The primary term is stored as
 * a doc value field without being indexed, since it is only intended for use
 * as a key-value lookup.

 */
public class MappingVersionFieldMapper extends MetadataFieldMapper {

    /**
     * A sequence ID, which is made up of a sequence number (both the searchable
     * and doc_value version of the field) and the primary term.
     */
    public static class MappingVersionFields {

        public final Field mappingVersion;
        //public final Field tombstoneField;

        private MappingVersionFields(Field mappingVersion) {
            Objects.requireNonNull(mappingVersion, "sequence number field cannot be null");
            this.mappingVersion = mappingVersion;
            //this.tombstoneField = tombstoneField;
        }

        public void addFields(LuceneDocument document) {
            document.add(mappingVersion);
//            if (tombstoneField != null) {
//                document.add(tombstoneField);
//            }
        }

        public static MappingVersionFields emptyMappingVersion() {
            return new MappingVersionFields(
                new LongPoint(NAME, 0)
            );
        }

        public static MappingVersionFields tombstone() {
            return new MappingVersionFields(
                new LongPoint(NAME, 0)
            );
        }
    }

    public static final String NAME = "_mappingVersion";
    public static final String CONTENT_TYPE = "_mappingVersion";
//    public static final String TOMBSTONE_NAME = "_tombstone";

    public static final MappingVersionFieldMapper INSTANCE = new MappingVersionFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    /**consider extends again*/
    static final class MappingVersionFieldType extends SimpleMappedFieldType {

        private static final MappingVersionFieldType INSTANCE = new MappingVersionFieldType();

        /**consider text search info*/
        private MappingVersionFieldType() {
            super(NAME, true, false, true, TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private long parse(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                }
                if (doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Long.parseLong(value.toString());
        }

        /**see value fetcher*/
        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            long v = parse(value);
            return LongPoint.newExactQuery(name(), v);
        }

        @Override
        public Query termsQuery(Collection<?> values, @Nullable SearchExecutionContext context) {
            long[] v = values.stream().mapToLong(this::parse).toArray();
            return LongPoint.newSetQuery(name(), v);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = parse(lowerTerm);
                if (includeLower == false) {
                    if (l == Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = parse(upperTerm);
                if (includeUpper == false) {
                    if (u == Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return LongPoint.newRangeQuery(name(), l, u);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.LONG);
        }
    }

    private MappingVersionFieldMapper() {
        super(MappingVersionFieldType.INSTANCE);
    }

    @Override
    public void preParse(DocumentParserContext context) {
        // see InternalEngine.innerIndex to see where the real version value is set
        // also see ParsedDocument.updateSeqID (called by innerIndex)
        //SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        MappingVersionFields mappingVersion = MappingVersionFields.emptyMappingVersion();
        /**see what to add here*/
        context.mappingVersion(mappingVersion);
        mappingVersion.addFields(context.doc());
        //context.doc().add((IndexableField) mappingVersion);
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        /**consider again*/
        MappingVersionFields mappingVersion = context.mappingVersion();
        assert mappingVersion != null;
        for (LuceneDocument doc : context.nonRootDocuments()) {
            doc.add(mappingVersion.mappingVersion);

        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
