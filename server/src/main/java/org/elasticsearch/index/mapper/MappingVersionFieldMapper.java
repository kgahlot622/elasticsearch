package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
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
 * Mapper for the {@code _mapperVersion} field.
 */

public class MappingVersionFieldMapper extends MetadataFieldMapper {

    public static class MappingVersionFields {

        public final Field mappingVersion;
        public final Field mappingVersionDocValue;

        private MappingVersionFields(Field mappingVersion, Field mappingVersionDocValue) {
            Objects.requireNonNull(mappingVersion, "sequence number field cannot be null");
            Objects.requireNonNull(mappingVersionDocValue, "mapping version dv cannot be null");
            this.mappingVersion = mappingVersion;
            this.mappingVersionDocValue = mappingVersionDocValue;
        }

        public void addFields(LuceneDocument document) {

            document.add(mappingVersion);
            document.add(mappingVersionDocValue);
        }

        public static MappingVersionFields emptyMappingVersion() {
            return new MappingVersionFields(
                new LongPoint(NAME, 0),
                new NumericDocValuesField(NAME, 0)
            );
        }

        public static MappingVersionFields tombstone() {
            return new MappingVersionFields(
                new LongPoint(NAME, 0),
                new NumericDocValuesField(NAME, 0)
            );
        }
    }

    public static final String NAME = "_mappingVersion";
    public static final String CONTENT_TYPE = "_mappingVersion";

    public static final MappingVersionFieldMapper INSTANCE = new MappingVersionFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static final class MappingVersionFieldType extends SimpleMappedFieldType {

        private static final MappingVersionFieldType INSTANCE = new MappingVersionFieldType();

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
        MappingVersionFields mappingVersion = MappingVersionFields.emptyMappingVersion();
        context.mappingVersion(mappingVersion);
        mappingVersion.addFields(context.doc());
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        MappingVersionFields mappingVersion = context.mappingVersion();
        assert mappingVersion != null;
        for (LuceneDocument doc : context.nonRootDocuments()) {
            doc.add(mappingVersion.mappingVersion);
            doc.add(mappingVersion.mappingVersionDocValue);

        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
