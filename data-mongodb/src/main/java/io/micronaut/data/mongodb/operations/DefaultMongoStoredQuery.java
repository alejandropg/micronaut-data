/*
 * Copyright 2017-2022 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.mongodb.operations;

import com.mongodb.client.model.Collation;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.util.StringUtils;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.document.model.query.builder.MongoQueryBuilder;
import io.micronaut.data.model.runtime.StoredQuery;
import io.micronaut.data.mongodb.annotation.MongoCollation;
import io.micronaut.data.runtime.query.internal.DelegateStoredQuery;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link MongoStoredQuery}.
 *
 * @param <E> The entity type
 * @param <R> The result type
 * @author Denis Stepanov
 * @since 3.3.
 */
@Internal
final class DefaultMongoStoredQuery<E, R> implements DelegateStoredQuery<E, R>, MongoStoredQuery<E, R> {

    private final StoredQuery<E, R> storedQuery;
    private final Bson update;
    private final boolean updateNeedsProcessing;
    private final List<Bson> pipeline;
    private final boolean pipelineNeedsProcessing;
    private final Bson filter;
    private final boolean filterNeedsProcessing;
    private final Bson collationAsBson;
    private final boolean collationNeedsProcessing;
    private final Collation collation;

    DefaultMongoStoredQuery(StoredQuery<E, R> storedQuery) {
        this(storedQuery, storedQuery.getAnnotationMetadata().stringValue(Query.class, "update").orElse(null));
    }

    DefaultMongoStoredQuery(StoredQuery<E, R> storedQuery, String updateJson) {
        this.storedQuery = storedQuery;
        this.update = updateJson == null ? null : BsonDocument.parse(updateJson);
        updateNeedsProcessing = update != null && needsProcessing(update);
        String query = storedQuery.getQuery();
        if (StringUtils.isEmpty(query)) {
            pipeline = null;
            filter = null;
        } else if (query.startsWith("[")) {
            pipeline = BsonArray.parse(query).stream().map(BsonValue::asDocument).collect(Collectors.toList());
            filter = null;
        } else {
            pipeline = null;
            filter = BsonDocument.parse(query);
        }
        filterNeedsProcessing = filter != null && needsProcessing(filter);
        pipelineNeedsProcessing = pipeline != null && needsProcessing(pipeline);
        collationAsBson = storedQuery.getAnnotationMetadata().stringValue(MongoCollation.class).map(BsonDocument::parse).orElse(null);
        collationNeedsProcessing = collationAsBson != null && needsProcessing(collationAsBson);
        collation = collationAsBson == null || collationNeedsProcessing ? null : MongoUtils.bsonDocumentAsCollation(collationAsBson.toBsonDocument());
    }

    @Override
    public boolean isUpdateNeedsProcessing() {
        return updateNeedsProcessing;
    }

    @Override
    public boolean isFilterNeedsProcessing() {
        return filterNeedsProcessing;
    }

    @Override
    public boolean isPipelineNeedsProcessing() {
        return pipelineNeedsProcessing;
    }

    private boolean needsProcessing(Bson value) {
        if (value instanceof BsonDocument) {
            return needsProcessingValue(value.toBsonDocument());
        }
        throw new IllegalStateException("Unrecognized value: " + value);
    }

    private boolean needsProcessing(List<Bson> values) {
        for (Bson value : values) {
            if (needsProcessing(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean needsProcessingValue(BsonValue value) {
        if (value instanceof BsonDocument) {
            BsonDocument bsonDocument = (BsonDocument) value;
            BsonInt32 queryParameterIndex = bsonDocument.getInt32(MongoQueryBuilder.QUERY_PARAMETER_PLACEHOLDER, null);
            if (queryParameterIndex != null) {
                return true;
            }
            for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
                BsonValue bsonValue = entry.getValue();
                if (needsProcessingValue(bsonValue)) {
                    return true;
                }
            }
            return false;
        } else if (value instanceof BsonArray) {
            BsonArray bsonArray = (BsonArray) value;
            for (BsonValue bsonValue : bsonArray) {
                if (needsProcessingValue(bsonValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Bson getUpdate() {
        return update;
    }

    @Override
    public Bson getFilter() {
        return filter;
    }

    @Override
    public List<Bson> getPipeline() {
        return pipeline;
    }

    @Override
    public Bson getCollationAsBson() {
        return collationAsBson;
    }

    @Override
    public boolean isCollationNeedsProcessing() {
        return collationNeedsProcessing;
    }

    @Override
    public Collation getCollation() {
        return collation;
    }

    @Override
    public StoredQuery<E, R> getStoredQueryDelegate() {
        return storedQuery;
    }
}
