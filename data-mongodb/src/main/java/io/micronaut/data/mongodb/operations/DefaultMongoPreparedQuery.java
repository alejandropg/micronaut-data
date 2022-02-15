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
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.beans.exceptions.IntrospectionException;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.data.document.model.query.builder.MongoQueryBuilder;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.Sort;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.data.mongodb.operations.options.MongoAggregationOptions;
import io.micronaut.data.mongodb.operations.options.MongoFindOptions;
import io.micronaut.data.runtime.query.internal.DelegateStoredQuery;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link MongoPreparedQuery}.
 *
 * @param <E>   The entity type
 * @param <R>   The result type
 * @param <Dtb> The database type
 * @author Denis Stepanov
 * @since 3.3.
 */
@Internal
final class DefaultMongoPreparedQuery<E, R, Dtb> implements DelegatePreparedQuery<E, R>, MongoPreparedQuery<E, R, Dtb> {

    private final PreparedQuery<E, R> preparedQuery;
    private final MongoStoredQuery<E, R> mongoStoredQuery;
    private final CodecRegistry codecRegistry;
    private final AttributeConverterRegistry attributeConverterRegistry;
    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final ConversionService<?> conversionService;
    private final Dtb database;
    private final RuntimePersistentEntity<E> persistentEntity;

    public DefaultMongoPreparedQuery(PreparedQuery<E, R> preparedQuery,
                                     CodecRegistry codecRegistry,
                                     AttributeConverterRegistry attributeConverterRegistry,
                                     RuntimeEntityRegistry runtimeEntityRegistry,
                                     RuntimePersistentEntity<E> persistentEntity,
                                     ConversionService<?> conversionService,
                                     Dtb database) {
        this.preparedQuery = preparedQuery;
        this.mongoStoredQuery = (MongoStoredQuery<E, R>) ((DelegateStoredQuery<Object, Object>) preparedQuery).getStoredQueryDelegate();
        this.codecRegistry = codecRegistry;
        this.attributeConverterRegistry = attributeConverterRegistry;
        this.runtimeEntityRegistry = runtimeEntityRegistry;
        this.conversionService = conversionService;
        this.database = database;
        this.persistentEntity = persistentEntity;
    }

    @Override
    public boolean isAggregate() {
        return mongoStoredQuery.isAggregate();
    }

    @Override
    public MongoAggregation getAggregation() {
        List<Bson> pipeline = getPipeline();
        if (pipeline == null) {
            throw new IllegalStateException("Pipeline query is not provided!");
        }
        MongoAggregationOptions options = new MongoAggregationOptions().collation(getCollation());
        return new MongoAggregation(pipeline, options);
    }

    @Override
    public MongoFind getFind() {
        MongoFindOptions options = new MongoFindOptions().filter(getFilter()).collation(getCollation()).sort(getSort()).projection(getProjection());
        Pageable pageable = preparedQuery.getPageable();
        if (pageable != Pageable.UNPAGED) {
            options = options.limit(pageable.getSize()).skip((int) pageable.getOffset());
            Sort pageableSort = pageable.getSort();
            if (pageableSort.isSorted()) {
                Bson sort = pageableSort.getOrderBy().stream().map(order -> order.isAscending() ? Sorts.ascending(order.getProperty()) : Sorts.descending(order.getProperty())).collect(Collectors.collectingAndThen(Collectors.toList(), Sorts::orderBy));
                options = options.sort(sort);
            }
        }
        return new MongoFind(options);
    }

    @Override
    public MongoUpdateMany getUpdateMany() {
        Bson update = getUpdate();
        if (update == null) {
            throw new IllegalStateException("Update query is not provided!");
        }
        UpdateOptions options = new UpdateOptions().collation(getCollation());
        return new MongoUpdateMany(update, getFilterOrEmpty(), options);
    }

    @Override
    public MongoDeleteMany getDeleteMany() {
        DeleteOptions options = new DeleteOptions().collation(getCollation());
        return new MongoDeleteMany(getFilterOrEmpty(), options);
    }

    @Override
    public RuntimePersistentEntity<E> getRuntimePersistentEntity() {
        return persistentEntity;
    }

    @Override
    public Dtb getDatabase() {
        return database;
    }

    @Override
    public PreparedQuery<E, R> getPreparedQueryDelegate() {
        return preparedQuery;
    }

    @Override
    public Collation getCollation() {
        Collation collation = mongoStoredQuery.getCollation();
        if (collation != null) {
            return collation;
        }

        Bson collationAsBson = getCollationAsBson();
        if (collationAsBson == null) {
            return null;
        }
       return MongoUtils.bsonDocumentAsCollation(collationAsBson.toBsonDocument());
    }

    @Override
    public Bson getCollationAsBson() {
        Bson collation = mongoStoredQuery.getCollationAsBson();
        if (collation == null) {
            return null;
        }
        return mongoStoredQuery.isCollationNeedsProcessing() ? replaceQueryParameters(collation) : collation;
    }

    @Override
    public boolean isCollationNeedsProcessing() {
        return mongoStoredQuery.isCollationNeedsProcessing();
    }

    @Override
    public Bson getFilter() {
        Bson filter = mongoStoredQuery.getFilter();
        if (filter == null) {
            return null;
        }
        return mongoStoredQuery.isFilterNeedsProcessing() ? replaceQueryParameters(filter) : filter;
    }

    @Override
    public Bson getSort() {
        Bson sort = mongoStoredQuery.getSort();
        if (sort == null) {
            return null;
        }
        return mongoStoredQuery.isSortNeedsProcessing() ? replaceQueryParameters(sort) : sort;
    }

    @Override
    public boolean isSortNeedsProcessing() {
        return mongoStoredQuery.isSortNeedsProcessing();
    }

    @Override
    public Bson getProjection() {
        Bson projection = mongoStoredQuery.getProjection();
        if (projection == null) {
            return null;
        }
        return mongoStoredQuery.isProjectionNeedsProcessing() ? replaceQueryParameters(projection) : projection;
    }

    @Override
    public boolean isProjectionNeedsProcessing() {
        return mongoStoredQuery.isProjectionNeedsProcessing();
    }

    @Override
    public boolean isFilterNeedsProcessing() {
        return mongoStoredQuery.isFilterNeedsProcessing();
    }

    @Override
    public List<Bson> getPipeline() {
        List<Bson> pipeline = mongoStoredQuery.getPipeline();
        if (pipeline == null) {
            return null;
        }
        Pageable pageable = getPageable();
        if (pageable != Pageable.UNPAGED) {
            pipeline = new ArrayList<>(pipeline);
            applyPageable(pageable, pipeline);
        }
        return mongoStoredQuery.isPipelineNeedsProcessing() ? replaceQueryParametersInList(pipeline) : pipeline;
    }

    @Override
    public boolean isPipelineNeedsProcessing() {
        return mongoStoredQuery.isPipelineNeedsProcessing();
    }

    @Override
    public Bson getUpdate() {
        Bson update = mongoStoredQuery.getUpdate();
        if (update == null) {
            return null;
        }
        return mongoStoredQuery.isUpdateNeedsProcessing() ? replaceQueryParameters(update) : update;
    }

    @Override
    public boolean isUpdateNeedsProcessing() {
        return mongoStoredQuery.isUpdateNeedsProcessing();
    }

    private Bson replaceQueryParameters(Bson value) {
        if (value instanceof BsonDocument) {
            return (BsonDocument) replaceQueryParametersInBsonValue(((BsonDocument) value).clone());
        }
        throw new IllegalStateException("Unrecognized value: " + value);
    }

    private <T> List<Bson> replaceQueryParametersInList(List<Bson> values) {
        values = new ArrayList<>(values);
        for (int i = 0; i < values.size(); i++) {
            Bson value = values.get(i);
            Bson newValue = replaceQueryParameters(value);
            if (value != newValue) {
                values.set(i, newValue);
            }
        }
        return values;
    }

    private BsonValue replaceQueryParametersInBsonValue(BsonValue value) {
        if (value instanceof BsonDocument) {
            BsonDocument bsonDocument = (BsonDocument) value;
            BsonInt32 queryParameterIndex = bsonDocument.getInt32(MongoQueryBuilder.QUERY_PARAMETER_PLACEHOLDER, null);
            if (queryParameterIndex != null) {
                int index = queryParameterIndex.getValue();
                return getValue(index, preparedQuery.getQueryBindings().get(index), preparedQuery, persistentEntity, codecRegistry);
            }
            for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
                BsonValue bsonValue = entry.getValue();
                BsonValue newValue = replaceQueryParametersInBsonValue(bsonValue);
                if (bsonValue != newValue) {
                    entry.setValue(newValue);
                }
            }
            return bsonDocument;
        } else if (value instanceof BsonArray) {
            BsonArray bsonArray = (BsonArray) value;
            for (int i = 0; i < bsonArray.size(); i++) {
                BsonValue bsonValue = bsonArray.get(i);
                BsonValue newValue = replaceQueryParametersInBsonValue(bsonValue);
                if (bsonValue != newValue) {
                    if (newValue.isNull()) {
                        bsonArray.remove(i);
                        i -= 1;
                    } else if (newValue.isArray()) {
                        bsonArray.remove(i);
                        List<BsonValue> values = newValue.asArray().getValues();
                        bsonArray.addAll(i, values);
                        i += values.size() - 1;
                    } else {
                        bsonArray.set(i, newValue);
                    }
                }
            }
        }
        return value;
    }

    private <T> BsonValue getValue(int index,
                                   QueryParameterBinding queryParameterBinding,
                                   PreparedQuery<?, ?> preparedQuery,
                                   RuntimePersistentEntity<T> persistentEntity,
                                   CodecRegistry codecRegistry) {
        Class<?> parameterConverter = queryParameterBinding.getParameterConverterClass();
        Object value;
        if (queryParameterBinding.getParameterIndex() != -1) {
            value = resolveParameterValue(queryParameterBinding, preparedQuery.getParameterArray());
        } else if (queryParameterBinding.isAutoPopulated()) {
            PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
            RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
            Object previousValue = null;
            QueryParameterBinding previousPopulatedValueParameter = queryParameterBinding.getPreviousPopulatedValueParameter();
            if (previousPopulatedValueParameter != null) {
                if (previousPopulatedValueParameter.getParameterIndex() == -1) {
                    throw new IllegalStateException("Previous value parameter cannot be bind!");
                }
                previousValue = resolveParameterValue(previousPopulatedValueParameter, preparedQuery.getParameterArray());
            }
            value = runtimeEntityRegistry.autoPopulateRuntimeProperty(persistentProperty, previousValue);
            value = convert(value, persistentProperty);
            parameterConverter = null;
        } else {
            throw new IllegalStateException("Invalid query [" + "]. Unable to establish parameter value for parameter at position: " + (index + 1));
        }

        DataType dataType = queryParameterBinding.getDataType();
        List<Object> values = expandValue(value, dataType);
        if (values != null && values.isEmpty()) {
            // Empty collections / array should always set at least one value
            value = null;
            values = null;
        }
        if (values == null) {
            if (parameterConverter != null) {
                int parameterIndex = queryParameterBinding.getParameterIndex();
                Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
                value = convert(parameterConverter, value, argument);
            }
            if (value instanceof String) {
                PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
                RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
                if (persistentProperty instanceof RuntimeAssociation) {
                    RuntimeAssociation runtimeAssociation = (RuntimeAssociation) persistentProperty;
                    RuntimePersistentProperty identity = runtimeAssociation.getAssociatedEntity().getIdentity();
                    if (identity != null && identity.getType() == String.class && identity.isGenerated()) {
                        return new BsonObjectId(new ObjectId((String) value));
                    }
                }
                if (persistentProperty.getOwner().getIdentity() == persistentProperty && persistentProperty.getType() == String.class && persistentProperty.isGenerated()) {
                    return new BsonObjectId(new ObjectId((String) value));
                }
            }
            return MongoUtils.toBsonValue(conversionService, value, codecRegistry);
        } else {
            Class<?> finalParameterConverter = parameterConverter;
            return new BsonArray(values.stream().map(val -> {
                if (finalParameterConverter != null) {
                    int parameterIndex = queryParameterBinding.getParameterIndex();
                    Argument<?> argument = parameterIndex > -1 ? preparedQuery.getArguments()[parameterIndex] : null;
                    val = convert(finalParameterConverter, val, argument);
                }
                return MongoUtils.toBsonValue(conversionService, val, codecRegistry);
            }).collect(Collectors.toList()));
        }
    }

    private int applyPageable(Pageable pageable, List<Bson> pipeline) {
        int limit = 0;
        if (pageable != Pageable.UNPAGED) {
            int skip = (int) pageable.getOffset();
            limit = pageable.getSize();
            Sort pageableSort = pageable.getSort();
            if (pageableSort.isSorted()) {
                Bson sort = pageableSort.getOrderBy().stream().map(order -> order.isAscending() ? Sorts.ascending(order.getProperty()) : Sorts.descending(order.getProperty())).collect(Collectors.collectingAndThen(Collectors.toList(), Sorts::orderBy));
                BsonDocument sortStage = new BsonDocument().append("$sort", sort.toBsonDocument());
                addStageToPipelineBefore(pipeline, sortStage, "$limit", "$skip");
            }
            if (skip > 0) {
                pipeline.add(new BsonDocument().append("$skip", new BsonInt32(skip)));
            }
            if (limit > 0) {
                pipeline.add(new BsonDocument().append("$limit", new BsonInt32(limit)));
            }
        }
        return limit;
    }

    private void addStageToPipelineBefore(List<Bson> pipeline, BsonDocument stageToAdd, String... beforeStages) {
        int lastFoundIndex = -1;
        int index = 0;
        for (Bson stage : pipeline) {
            for (String beforeStageName : beforeStages) {
                if (stage.toBsonDocument().containsKey(beforeStageName)) {
                    lastFoundIndex = index;
                    break;
                }
            }
            index++;
        }
        if (lastFoundIndex > -1) {
            pipeline.add(lastFoundIndex, stageToAdd);
        } else {
            pipeline.add(stageToAdd);
        }
    }

    private Object convert(Class<?> converterClass, Object value, @Nullable Argument<?> argument) {
        if (converterClass == null) {
            return value;
        }
        AttributeConverter<Object, Object> converter = attributeConverterRegistry.getConverter(converterClass);
        ConversionContext conversionContext = createTypeConversionContext(null, argument);
        return converter.convertToPersistedValue(value, conversionContext);
    }

    private Object convert(Object value, RuntimePersistentProperty<?> property) {
        AttributeConverter<Object, Object> converter = property.getConverter();
        if (converter != null) {
            return converter.convertToPersistedValue(value, createTypeConversionContext(property, property.getArgument()));
        }
        return value;
    }

    private <T> PersistentPropertyPath getRequiredPropertyPath(QueryParameterBinding queryParameterBinding, RuntimePersistentEntity<T> persistentEntity) {
        String[] propertyPath = queryParameterBinding.getRequiredPropertyPath();
        PersistentPropertyPath pp = persistentEntity.getPropertyPath(propertyPath);
        if (pp == null) {
            throw new IllegalStateException("Cannot find auto populated property: " + String.join(".", propertyPath));
        }
        return pp;
    }

    private List<Object> expandValue(Object value, DataType dataType) {
        // Special case for byte array, we want to support a list of byte[] convertible values
        if (value == null || dataType != null && dataType.isArray() && dataType != DataType.BYTE_ARRAY || value instanceof byte[]) {
            // not expanded
            return null;
        } else if (value instanceof Iterable) {
            return (List<Object>) CollectionUtils.iterableToList((Iterable<?>) value);
        } else if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            if (len == 0) {
                return Collections.emptyList();
            } else {
                List<Object> list = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    Object o = Array.get(value, j);
                    list.add(o);
                }
                return list;
            }
        } else {
            // not expanded
            return null;
        }
    }

    private Object resolveParameterValue(QueryParameterBinding queryParameterBinding, Object[] parameterArray) {
        Object value;
        value = parameterArray[queryParameterBinding.getParameterIndex()];
        String[] parameterBindingPath = queryParameterBinding.getParameterBindingPath();
        if (parameterBindingPath != null) {
            for (String prop : parameterBindingPath) {
                if (value == null) {
                    return null;
                }
                Object finalValue = value;
                BeanProperty beanProperty = BeanIntrospection.getIntrospection(value.getClass())
                        .getProperty(prop).orElseThrow(() -> new IntrospectionException("Cannot find a property: '" + prop + "' on bean: " + finalValue));
                value = beanProperty.get(value);
            }
        }
        return value;
    }

    private ConversionContext createTypeConversionContext(RuntimePersistentProperty<?> property, Argument<?> argument) {
        if (argument != null) {
            return ConversionContext.of(argument);
        }
        return ConversionContext.DEFAULT;
    }

}
