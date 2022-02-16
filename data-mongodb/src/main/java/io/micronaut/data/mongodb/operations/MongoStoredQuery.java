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
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.data.model.runtime.StoredQuery;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * MongoDB's {@link StoredQuery}.
 *
 * @param <E> The entity type
 * @param <R> The result type
 * @author Denis Stepanov
 * @since 3.3.
 */
@Experimental
public interface MongoStoredQuery<E, R> extends StoredQuery<E, R> {

    default boolean isAggregate() {
        return false;
    }

    default MongoAggregation getAggregation() {
        return null;
    }

    default MongoFind getFind() {
        return null;
    }

    default MongoUpdate getUpdateMany() {
        return null;
    }

    default MongoUpdate getUpdateOne(E entity) {
        return null;
    }

    default MongoDeleteMany getDeleteMany() {
        return null;
    }

    /**
     * The filter.
     * NOTE: The value shouldn't be modified.
     *
     * @return The filter
     */
    @Nullable
    Bson getFilter();

    /**
     * @return true if filter value needs to replace query parameter values.
     */
    boolean isFilterNeedsProcessing();

    /**
     * The sort.
     * NOTE: The value shouldn't be modified.
     *
     * @return The sort
     */
    @Nullable
    Bson getSort();

    /**
     * @return true if sort value needs to replace query parameter values.
     */
    boolean isSortNeedsProcessing();

    /**
     * The projection.
     * NOTE: The value shouldn't be modified.
     *
     * @return The projection
     */
    @Nullable
    Bson getProjection();

    /**
     * @return true if projection value needs to replace query parameter values.
     */
    boolean isProjectionNeedsProcessing();

    /**
     * The filter or empty.
     * NOTE: The value shouldn't be modified.
     *
     * @return The filter
     */
    @NonNull
    default Bson getFilterOrEmpty() {
        Bson filter = getFilter();
        return filter == null ? new BsonDocument() : filter;
    }

    /**
     * The aggregation pipeline.
     * NOTE: The value shouldn't be modified.
     *
     * @return The pipeline
     */
    @Nullable
    List<Bson> getPipeline();

    /**
     * @return true if pipeline value needs to replace query parameter values.
     */
    boolean isPipelineNeedsProcessing();

    /**
     * The update.
     * NOTE: The value shouldn't be modified.
     *
     * @return The update
     */
    @Nullable
    Bson getUpdate();

    /**
     * @return true if update value needs to replace query parameter values.
     */
    boolean isUpdateNeedsProcessing();

    /**
     * The Bson.
     * NOTE: The value shouldn't be modified.
     *
     * @return The Bson
     */
    @Nullable
    default Bson getCollationAsBson() {
        return null;
    }

    /**
     * @return true if collation value needs to replace query parameter values.
     */
    default boolean isCollationNeedsProcessing() {
        return false;
    }

    @Nullable
    default Collation getCollation() {
        return null;
    }

}
