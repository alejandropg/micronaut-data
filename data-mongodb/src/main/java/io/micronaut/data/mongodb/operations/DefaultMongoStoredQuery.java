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
import com.mongodb.client.model.UpdateOptions;
import io.micronaut.aop.InvocationContext;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.beans.exceptions.IntrospectionException;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.document.model.query.builder.MongoQueryBuilder;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.QueryParameterBinding;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.StoredQuery;
import io.micronaut.data.model.runtime.convert.AttributeConverter;
import io.micronaut.data.mongodb.annotation.MongoCollation;
import io.micronaut.data.mongodb.annotation.MongoProjection;
import io.micronaut.data.mongodb.annotation.MongoSort;
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
 * Default implementation of {@link MongoStoredQuery}.
 *
 * @param <E>   The entity type
 * @param <R>   The result type
 * @param <Dtb> The database type
 * @author Denis Stepanov
 * @since 3.3.
 */
@Internal
final class DefaultMongoStoredQuery<E, R, Dtb> implements DelegateStoredQuery<E, R>, MongoStoredQuery<E, R, Dtb> {

    private final StoredQuery<E, R> storedQuery;
    private final CodecRegistry codecRegistry;
    private final AttributeConverterRegistry attributeConverterRegistry;
    private final RuntimeEntityRegistry runtimeEntityRegistry;
    private final ConversionService<?> conversionService;
    private final Dtb database;
    private final RuntimePersistentEntity<E> persistentEntity;
    private final Bson update;
    private final boolean updateNeedsProcessing;
    private final List<Bson> pipeline;
    private final boolean pipelineNeedsProcessing;
    private final Bson filter;
    private final boolean filterNeedsProcessing;
    private final Bson collationAsBson;
    private final boolean collationNeedsProcessing;
    private final Collation collation;
    private final boolean sortNeedsProcessing;
    private final Bson sort;
    private final boolean projectionNeedsProcessing;
    private final Bson projection;
    private final boolean isAggregate;

    DefaultMongoStoredQuery(StoredQuery<E, R> storedQuery,
                            CodecRegistry codecRegistry,
                            AttributeConverterRegistry attributeConverterRegistry,
                            RuntimeEntityRegistry runtimeEntityRegistry,
                            ConversionService<?> conversionService,
                            RuntimePersistentEntity<E> persistentEntity,
                            Dtb database) {
        this(storedQuery,
                codecRegistry,
                attributeConverterRegistry,
                runtimeEntityRegistry,
                conversionService,
                persistentEntity,
                database,
                storedQuery.getAnnotationMetadata().stringValue(Query.class, "update").orElse(null));
    }

    DefaultMongoStoredQuery(StoredQuery<E, R> storedQuery,
                            CodecRegistry codecRegistry,
                            AttributeConverterRegistry attributeConverterRegistry,
                            RuntimeEntityRegistry runtimeEntityRegistry,
                            ConversionService<?> conversionService,
                            RuntimePersistentEntity<E> persistentEntity,
                            Dtb database,
                            String updateJson) {
        this.storedQuery = storedQuery;
        this.codecRegistry = codecRegistry;
        this.attributeConverterRegistry = attributeConverterRegistry;
        this.runtimeEntityRegistry = runtimeEntityRegistry;
        this.conversionService = conversionService;
        this.update = updateJson == null ? null : BsonDocument.parse(updateJson);
        this.database = database;
        this.persistentEntity = persistentEntity;
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
        sort = storedQuery.getAnnotationMetadata().stringValue(MongoSort.class).map(BsonDocument::parse).orElse(null);
        sortNeedsProcessing = sort != null && needsProcessing(sort);
        projection = storedQuery.getAnnotationMetadata().stringValue(MongoProjection.class).map(BsonDocument::parse).orElse(null);
        projectionNeedsProcessing = projection != null && needsProcessing(projection);
        isAggregate = pipeline != null;
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
    public boolean isAggregate() {
        return isAggregate;
    }

    @Override
    public MongoAggregation getAggregation(InvocationContext<?, ?> invocationContext) {
        List<Bson> pipeline = getPipeline(invocationContext);
        if (pipeline == null) {
            throw new IllegalStateException("Pipeline query is not provided!");
        }
        MongoAggregationOptions options = new MongoAggregationOptions().collation(getCollation(invocationContext, null));
        return new MongoAggregation(pipeline, options);
    }

    @Override
    public MongoFind getFind(InvocationContext<?, ?> invocationContext) {
        MongoFindOptions options = new MongoFindOptions()
                .filter(getFilter(invocationContext, null))
                .collation(getCollation(invocationContext, null))
                .sort(getSort(invocationContext, null))
                .projection(getProjection(invocationContext, null));
        return new MongoFind(options);
    }

    @Override
    public MongoUpdate getUpdateMany(InvocationContext<?, ?> invocationContext) {
        Bson update = getUpdate(invocationContext, null);
        if (update == null) {
            throw new IllegalStateException("Update query is not provided!");
        }
        UpdateOptions options = new UpdateOptions().collation(getCollation(invocationContext, null));
        return new MongoUpdate(update, getFilterOrEmpty(invocationContext, null), options);
    }

    @Override
    public MongoUpdate getUpdateOne(E entity) {
        Bson update = getUpdate(null, entity);
        if (update == null) {
            throw new IllegalStateException("Update query is not provided!");
        }
        UpdateOptions options = new UpdateOptions().collation(getCollation(null, null));
        return new MongoUpdate(update, getFilterOrEmpty(null, entity), options);
    }

    @Override
    public MongoDelete getDeleteMany(InvocationContext<?, ?> invocationContext) {
        DeleteOptions options = new DeleteOptions().collation(getCollation(invocationContext, null));
        return new MongoDelete(getFilterOrEmpty(invocationContext, null), options);
    }

    @Override
    public MongoDelete getDeleteOne(E entity) {
        DeleteOptions options = new DeleteOptions().collation(getCollation(null, null));
        return new MongoDelete(getFilterOrEmpty(null, entity), options);
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

    private Bson getUpdate(@Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (update == null) {
            throw new IllegalStateException("Update query is not provided!");
        }
        return updateNeedsProcessing ? replaceQueryParameters(update, invocationContext, entity) : update;
    }

    private Bson getFilterOrEmpty(@Nullable InvocationContext<?, ?> invocationContext, E entity) {
        Bson filter = getFilter(invocationContext, entity);
        if (filter == null) {
            return new BsonDocument();
        }
        return filter;
    }

    private Bson getFilter(@Nullable InvocationContext<?, ?> invocationContext, E entity) {
        if (filter == null) {
            return null;
        }
        return filterNeedsProcessing ? replaceQueryParameters(filter, invocationContext, entity) : filter;
    }

    private Collation getCollation(@Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (collation != null) {
            return collation;
        }
        if (collationAsBson == null) {
            return null;
        }
        Bson collationAsBson = collationNeedsProcessing ? replaceQueryParameters(this.collationAsBson, invocationContext, entity) : this.collationAsBson;
        return MongoUtils.bsonDocumentAsCollation(collationAsBson.toBsonDocument());
    }

    private Bson getSort(@Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (sort == null) {
            return null;
        }
        return sortNeedsProcessing ? replaceQueryParameters(sort, invocationContext, entity) : sort;
    }

    private Bson getProjection(@Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (projection == null) {
            return null;
        }
        return projectionNeedsProcessing ? replaceQueryParameters(projection, invocationContext, entity) : projection;
    }

    private List<Bson> getPipeline(@Nullable InvocationContext<?, ?> invocationContext) {
        if (pipeline == null) {
            return null;
        }
//        Pageable pageable = getPageable();
//        if (pageable != Pageable.UNPAGED) {
//            pipeline = new ArrayList<>(pipeline);
//            applyPageable(pageable, pipeline);
//        }
        return pipelineNeedsProcessing ? replaceQueryParametersInList(pipeline, invocationContext, null) : pipeline;
    }

    private Bson replaceQueryParameters(Bson value, @Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (value instanceof BsonDocument) {
            return (BsonDocument) replaceQueryParametersInBsonValue(((BsonDocument) value).clone(), invocationContext, entity);
        }
        throw new IllegalStateException("Unrecognized value: " + value);
    }

    private <T> List<Bson> replaceQueryParametersInList(List<Bson> values, @Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        values = new ArrayList<>(values);
        for (int i = 0; i < values.size(); i++) {
            Bson value = values.get(i);
            Bson newValue = replaceQueryParameters(value, invocationContext, entity);
            if (value != newValue) {
                values.set(i, newValue);
            }
        }
        return values;
    }

    private BsonValue replaceQueryParametersInBsonValue(BsonValue value, @Nullable InvocationContext<?, ?> invocationContext, @Nullable E entity) {
        if (value instanceof BsonDocument) {
            BsonDocument bsonDocument = (BsonDocument) value;
            BsonInt32 queryParameterIndex = bsonDocument.getInt32(MongoQueryBuilder.QUERY_PARAMETER_PLACEHOLDER, null);
            if (queryParameterIndex != null) {
                int index = queryParameterIndex.getValue();
                return getValue(index, getQueryBindings().get(index), invocationContext, persistentEntity, codecRegistry, entity);
            }
            for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
                BsonValue bsonValue = entry.getValue();
                BsonValue newValue = replaceQueryParametersInBsonValue(bsonValue, invocationContext, entity);
                if (bsonValue != newValue) {
                    entry.setValue(newValue);
                }
            }
            return bsonDocument;
        } else if (value instanceof BsonArray) {
            BsonArray bsonArray = (BsonArray) value;
            for (int i = 0; i < bsonArray.size(); i++) {
                BsonValue bsonValue = bsonArray.get(i);
                BsonValue newValue = replaceQueryParametersInBsonValue(bsonValue, invocationContext, entity);
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

    private <QE, QR, T> BsonValue getValue(int index,
                                           QueryParameterBinding queryParameterBinding,
                                           InvocationContext<?, ?> invocationContext,
                                           RuntimePersistentEntity<T> persistentEntity,
                                           CodecRegistry codecRegistry, E entity) {
        Class<?> parameterConverter = queryParameterBinding.getParameterConverterClass();
        Object value;
        if (queryParameterBinding.getParameterIndex() != -1) {
            requireInvocationContext(invocationContext);
            value = resolveParameterValue(queryParameterBinding, invocationContext.getParameterValues());
        } else if (queryParameterBinding.isAutoPopulated()) {
            PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
            RuntimePersistentProperty<?> persistentProperty = (RuntimePersistentProperty) pp.getProperty();
            Object previousValue = null;
            QueryParameterBinding previousPopulatedValueParameter = queryParameterBinding.getPreviousPopulatedValueParameter();
            if (previousPopulatedValueParameter != null) {
                if (previousPopulatedValueParameter.getParameterIndex() == -1) {
                    throw new IllegalStateException("Previous value parameter cannot be bind!");
                }
                previousValue = resolveParameterValue(previousPopulatedValueParameter, invocationContext.getParameterValues());
            }
            value = runtimeEntityRegistry.autoPopulateRuntimeProperty(persistentProperty, previousValue);
            value = convert(value, persistentProperty);
            parameterConverter = null;
        } else if (entity != null) {
            PersistentPropertyPath pp = getRequiredPropertyPath(queryParameterBinding, persistentEntity);
            value = pp.getPropertyValue(entity);
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
                Argument<?> argument;
                if (parameterIndex > -1) {
                    requireInvocationContext(invocationContext);
                    argument = invocationContext.getArguments()[parameterIndex];
                } else {
                    argument = null;
                }
                value = convert(parameterConverter, value, argument);
            }
            // Check if the parameter is not an id which might be represented as String but needs to mapped as ObjectId
            if (value instanceof String && queryParameterBinding.getPropertyPath() != null) {
                // TODO: improve id recognition
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
                    Argument<?> argument;
                    if (parameterIndex > -1) {
                        requireInvocationContext(invocationContext);
                        argument = invocationContext.getArguments()[parameterIndex];
                    } else {
                        argument = null;
                    }
                    val = convert(finalParameterConverter, val, argument);
                }
                return MongoUtils.toBsonValue(conversionService, val, codecRegistry);
            }).collect(Collectors.toList()));
        }
    }

    private void requireInvocationContext(InvocationContext<?, ?> invocationContext) {
        if (invocationContext == null) {
            throw new IllegalStateException("Invocation context is required!");
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

    @Override
    public StoredQuery<E, R> getStoredQueryDelegate() {
        return storedQuery;
    }
}
