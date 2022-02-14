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

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.data.model.DataType;
import io.micronaut.data.model.Page;
import io.micronaut.data.model.Pageable;
import io.micronaut.data.model.PersistentProperty;
import io.micronaut.data.model.Sort;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.model.runtime.AttributeConverterRegistry;
import io.micronaut.data.model.runtime.DeleteBatchOperation;
import io.micronaut.data.model.runtime.DeleteOperation;
import io.micronaut.data.model.runtime.InsertBatchOperation;
import io.micronaut.data.model.runtime.InsertOperation;
import io.micronaut.data.model.runtime.PagedQuery;
import io.micronaut.data.model.runtime.PreparedQuery;
import io.micronaut.data.model.runtime.RuntimeAssociation;
import io.micronaut.data.model.runtime.RuntimeEntityRegistry;
import io.micronaut.data.model.runtime.RuntimePersistentEntity;
import io.micronaut.data.model.runtime.RuntimePersistentProperty;
import io.micronaut.data.model.runtime.UpdateBatchOperation;
import io.micronaut.data.model.runtime.UpdateOperation;
import io.micronaut.data.mongodb.conf.RequiresSyncMongo;
import io.micronaut.data.mongodb.database.MongoDatabaseFactory;
import io.micronaut.data.mongodb.transaction.MongoSynchronousTransactionManager;
import io.micronaut.data.operations.async.AsyncCapableRepository;
import io.micronaut.data.operations.reactive.ReactiveCapableRepository;
import io.micronaut.data.operations.reactive.ReactiveRepositoryOperations;
import io.micronaut.data.runtime.convert.DataConversionService;
import io.micronaut.data.runtime.date.DateTimeProvider;
import io.micronaut.data.runtime.operations.ExecutorAsyncOperations;
import io.micronaut.data.runtime.operations.ExecutorReactiveOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntitiesOperations;
import io.micronaut.data.runtime.operations.internal.AbstractSyncEntityOperations;
import io.micronaut.data.runtime.operations.internal.OperationContext;
import io.micronaut.data.runtime.operations.internal.SyncCascadeOperations;
import io.micronaut.http.codec.MediaTypeCodec;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Named;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Default Mongo repository operations.
 *
 * @author Denis Stepanov
 * @since 3.3
 */
@RequiresSyncMongo
@EachBean(MongoClient.class)
@Internal
public final class DefaultMongoRepositoryOperations extends AbstractMongoRepositoryOperations<MongoDatabase, ClientSession, Object> implements
        MongoRepositoryOperations,
        AsyncCapableRepository,
        ReactiveCapableRepository,
        SyncCascadeOperations.SyncCascadeOperationsHelper<DefaultMongoRepositoryOperations.MongoOperationContext> {
    private final MongoClient mongoClient;
    private final SyncCascadeOperations<MongoOperationContext> cascadeOperations;
    private final MongoSynchronousTransactionManager transactionManager;
    private final MongoDatabaseFactory mongoDatabaseFactory;
    private ExecutorAsyncOperations asyncOperations;
    private ExecutorService executorService;

    /**
     * Default constructor.
     *
     * @param serverName                 The server name
     * @param beanContext                The bean context
     * @param codecs                     The media type codecs
     * @param dateTimeProvider           The date time provider
     * @param runtimeEntityRegistry      The entity registry
     * @param conversionService          The conversion service
     * @param attributeConverterRegistry The attribute converter registry
     * @param mongoClient                The Mongo client
     * @param executorService            The executor service
     */
    DefaultMongoRepositoryOperations(@Parameter String serverName,
                                               BeanContext beanContext,
                                               List<MediaTypeCodec> codecs,
                                               DateTimeProvider<Object> dateTimeProvider,
                                               RuntimeEntityRegistry runtimeEntityRegistry,
                                               DataConversionService<?> conversionService,
                                               AttributeConverterRegistry attributeConverterRegistry,
                                               MongoClient mongoClient,
                                               @Named("io") @Nullable ExecutorService executorService) {
        super(serverName, beanContext, codecs, dateTimeProvider, runtimeEntityRegistry, conversionService, attributeConverterRegistry);
        this.mongoClient = mongoClient;
        this.cascadeOperations = new SyncCascadeOperations<>(conversionService, this);
        boolean isPrimary = "Primary".equals(serverName);
        this.transactionManager = beanContext.getBean(MongoSynchronousTransactionManager.class, isPrimary ? null : Qualifiers.byName(serverName));
        this.mongoDatabaseFactory = beanContext.getBean(MongoDatabaseFactory.class, isPrimary ? null : Qualifiers.byName(serverName));
        this.executorService = executorService;
    }

    @Override
    public <T> T findOne(Class<T> type, Serializable id) {
        return withClientSession(clientSession -> {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(type);
            MongoDatabase database = getDatabase(persistentEntity, null);
            MongoCollection<T> collection = getCollection(database, persistentEntity, type);
            Bson filter = MongoUtils.filterById(conversionService, persistentEntity, id, collection.getCodecRegistry());
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
            }
            return collection.find(clientSession, filter, type).first();
        });
    }

    @Override
    public <T, R> R findOne(PreparedQuery<T, R> preparedQuery) {
        return withClientSession(clientSession -> {
            MongoPreparedQuery<T, R, MongoDatabase> mongoPreparedQuery = getMongoPreparedQuery(preparedQuery);
            if (isCountQuery(preparedQuery)) {
                return getCount(clientSession, mongoPreparedQuery);
            }
            List<Bson> pipeline = mongoPreparedQuery.getPipeline();
            if (pipeline == null) {
                return findOneFiltered(clientSession, mongoPreparedQuery);
            } else {
                return findOneAggregated(clientSession, mongoPreparedQuery, pipeline);
            }
        });
    }

    private <T, R> R getCount(ClientSession clientSession, MongoPreparedQuery<T, R, MongoDatabase> preparedQuery) {
        Class<R> resultType = preparedQuery.getResultType();
        RuntimePersistentEntity<T> persistentEntity = preparedQuery.getRuntimePersistentEntity();
        MongoDatabase database = preparedQuery.getDatabase();
        List<Bson> pipeline = preparedQuery.getPipeline();
        if (pipeline == null) {
            Bson filter = preparedQuery.getFilterOrEmpty();
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'countDocuments' with filter: {}", filter.toBsonDocument().toJson());
            }
            long count = getCollection(database, persistentEntity, BsonDocument.class)
                    .countDocuments(clientSession, filter);
            return conversionService.convertRequired(count, resultType);
        } else {
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
            }
            Class<T> type = preparedQuery.getRootEntity();
            R result = getCollection(database, persistentEntity, type)
                    .aggregate(clientSession, pipeline, BsonDocument.class)
                    .collation(preparedQuery.getCollation())
                    .map(bsonDocument -> convertResult(database.getCodecRegistry(), resultType, bsonDocument, false))
                    .first();
            if (result == null) {
                result = conversionService.convertRequired(0, resultType);
            }
            return result;
        }
    }

    @Override
    public <T> boolean exists(PreparedQuery<T, Boolean> preparedQuery) {
        return withClientSession(clientSession -> {
            MongoPreparedQuery<T, Boolean, MongoDatabase> mongoPreparedQuery = getMongoPreparedQuery(preparedQuery);
            Class<T> type = preparedQuery.getRootEntity();
            RuntimePersistentEntity<T> persistentEntity = mongoPreparedQuery.getRuntimePersistentEntity();
            MongoDatabase database = getDatabase(persistentEntity, preparedQuery.getRepositoryType());
            List<Bson> pipeline = mongoPreparedQuery.getPipeline();
            if (pipeline != null) {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
                }
                return getCollection(database, persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .aggregate(clientSession, pipeline)
                        .iterator().hasNext();
            } else {
                Bson filter = mongoPreparedQuery.getFilterOrEmpty();
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing exists Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
                }
                return getCollection(database, persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .find(clientSession, type)
                        .limit(1)
                        .filter(filter)
                        .collation(mongoPreparedQuery.getCollation())
                        .iterator().hasNext();
            }
        });
    }

    @Override
    public <T> Iterable<T> findAll(PagedQuery<T> query) {
        throw new DataAccessException("Not supported!");
    }

    @Override
    public <T> long count(PagedQuery<T> pagedQuery) {
        throw new DataAccessException("Not supported!");
    }

    @Override
    public <T> Stream<T> findStream(PagedQuery<T> query) {
        throw new DataAccessException("Not supported!");
    }

    @Override
    public <R> Page<R> findPage(PagedQuery<R> query) {
        throw new DataAccessException("Not supported!");
    }

    @Override
    public <T, R> Iterable<R> findAll(PreparedQuery<T, R> preparedQuery) {
        return withClientSession(clientSession -> findAllFiltered(clientSession, getMongoPreparedQuery(preparedQuery), false));
    }

    private <T, R> Iterable<R> findAllFiltered(ClientSession clientSession, MongoPreparedQuery<T, R, MongoDatabase> preparedQuery, boolean stream) {
        if (isCountQuery(preparedQuery)) {
            return Collections.singletonList(getCount(clientSession, preparedQuery));
        }
        List<Bson> pipeline = preparedQuery.getPipeline();
        if (pipeline == null) {
            return findAllFiltered(clientSession, preparedQuery, preparedQuery.getFilterOrEmpty(), stream);
        }
        return findAllAggregated(clientSession, preparedQuery, preparedQuery.isDtoProjection(), pipeline, stream);
    }

    @Override
    public <T, R> Stream<R> findStream(PreparedQuery<T, R> preparedQuery) {
        return withClientSession(clientSession -> {
            MongoIterable<R> iterable = (MongoIterable<R>) findAllFiltered(clientSession, getMongoPreparedQuery(preparedQuery), true);
            MongoCursor<R> iterator = iterable.iterator();
            Spliterators.AbstractSpliterator<R> spliterator = new Spliterators.AbstractSpliterator<R>(Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.IMMUTABLE) {
                @Override
                public boolean tryAdvance(Consumer<? super R> action) {
                    if (iterator.hasNext()) {
                        action.accept(iterator.next());
                        return true;
                    }
                    iterator.close();
                    return false;
                }
            };
            return StreamSupport.stream(spliterator, false).onClose(iterator::close);
        });
    }

    private <T, R> R findOneFiltered(ClientSession clientSession, MongoPreparedQuery<T, R, MongoDatabase> preparedQuery) {
        Bson filter = preparedQuery.getFilterOrEmpty();;
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'find' with filter: {}", filter.toBsonDocument().toJson());
        }
        Class<T> type = preparedQuery.getRootEntity();
        Class<R> resultType = preparedQuery.getResultType();
        RuntimePersistentEntity<T> persistentEntity = preparedQuery.getRuntimePersistentEntity();
        MongoDatabase database = preparedQuery.getDatabase();
        return getCollection(database, persistentEntity, resultType)
                .find(clientSession, filter, resultType)
                .collation(preparedQuery.getCollation())
                .limit(1)
                .map(r -> {
                    if (type.isInstance(r)) {
                        return (R) triggerPostLoad(preparedQuery.getAnnotationMetadata(), persistentEntity, type.cast(r));
                    }
                    return r;
                }).first();
    }

    private <T, R> R findOneAggregated(ClientSession clientSession, MongoPreparedQuery<T, R, MongoDatabase> preparedQuery, List<Bson> pipeline) {
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
        }
        MongoDatabase database = preparedQuery.getDatabase();
        RuntimePersistentEntity<T> persistentEntity = preparedQuery.getRuntimePersistentEntity();
        Class<T> type = preparedQuery.getRootEntity();
        Class<R> resultType = preparedQuery.getResultType();
        if (!resultType.isAssignableFrom(type)) {
            BsonDocument result = getCollection(database, persistentEntity, BsonDocument.class)
                    .aggregate(clientSession, pipeline, BsonDocument.class)
                    .collation(preparedQuery.getCollation())
                    .first();
            return convertResult(database.getCodecRegistry(), resultType, result, preparedQuery.isDtoProjection());
        }
        return getCollection(database, persistentEntity, resultType).aggregate(clientSession, pipeline)
                .collation(preparedQuery.getCollation())
                .map(r -> {
            if (type.isInstance(r)) {
                return (R) triggerPostLoad(preparedQuery.getAnnotationMetadata(), persistentEntity, type.cast(r));
            }
            return r;
        }).first();
    }

    private <T, R> Iterable<R> findAllAggregated(ClientSession clientSession,
                                                 MongoPreparedQuery<T, R, MongoDatabase> preparedQuery,
                                                 boolean isDtoProjection,
                                                 List<Bson> pipeline,
                                                 boolean stream) {
        Pageable pageable = preparedQuery.getPageable();
        int limit = pageable == Pageable.UNPAGED ? -1 : pageable.getSize();
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'aggregate' with pipeline: {}", pipeline.stream().map(e -> e.toBsonDocument().toJson()).collect(Collectors.toList()));
        }
        Collation collation = preparedQuery.getCollation();
        Class<T> type = preparedQuery.getRootEntity();
        Class<R> resultType = preparedQuery.getResultType();
        MongoIterable<R> aggregate;
        if (!resultType.isAssignableFrom(type)) {
            MongoDatabase database = preparedQuery.getDatabase();
            aggregate = getCollection(database, preparedQuery.getRuntimePersistentEntity(), BsonDocument.class)
                    .aggregate(clientSession, pipeline, BsonDocument.class)
                    .collation(collation)
                    .map(result -> convertResult(database.getCodecRegistry(), resultType, result, isDtoProjection));
        } else {
            aggregate = getCollection(preparedQuery.getDatabase(), preparedQuery.getRuntimePersistentEntity(), preparedQuery.getResultType())
                    .aggregate(clientSession, pipeline, resultType)
                    .collation(collation);
        }
        return stream ? aggregate : aggregate.into(new ArrayList<>(limit > 0 ? limit : 20));
    }

    private <T, R> Iterable<R> findAllFiltered(ClientSession clientSession,
                                               MongoPreparedQuery<T, R, MongoDatabase> preparedQuery,
                                               Bson filter,
                                               boolean stream) {
        Pageable pageable = preparedQuery.getPageable();
        Bson sort = null;
        int skip = 0;
        int limit = 0;
        if (pageable != Pageable.UNPAGED) {
            skip = (int) pageable.getOffset();
            limit = pageable.getSize();
            Sort pageableSort = pageable.getSort();
            if (pageableSort.isSorted()) {
                sort = pageableSort.getOrderBy().stream().map(order -> order.isAscending() ? Sorts.ascending(order.getProperty()) : Sorts.descending(order.getProperty())).collect(Collectors.collectingAndThen(Collectors.toList(), Sorts::orderBy));
            }
        }
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'find' with filter: {} skip: {} limit: {}", filter.toBsonDocument().toJson(), skip, limit);
        }
        Class<R> resultType = preparedQuery.getResultType();
        FindIterable<R> findIterable = getCollection(preparedQuery.getDatabase(), preparedQuery.getRuntimePersistentEntity(), resultType)
                .find(clientSession, filter, resultType)
                .skip(skip)
                .limit(Math.max(limit, 0))
                .sort(sort)
                .collation(preparedQuery.getCollation());
        return stream ? findIterable : findIterable.into(new ArrayList<>(limit > 0 ? limit : 20));
    }

    @Override
    public <T> T persist(InsertOperation<T> operation) {
        return withClientSession(clientSession -> {
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            return persistOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> Iterable<T> persistAll(InsertBatchOperation<T> operation) {
        return withClientSession(clientSession -> {
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            return persistBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()), null);
        });
    }

    @Override
    public <T> T update(UpdateOperation<T> operation) {
        return withClientSession(clientSession -> {
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            return updateOne(ctx, operation.getEntity(), runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> Iterable<T> updateAll(UpdateBatchOperation<T> operation) {
        return withClientSession(clientSession -> {
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            return updateBatch(ctx, operation, runtimeEntityRegistry.getEntity(operation.getRootEntity()));
        });
    }

    @Override
    public <T> int delete(DeleteOperation<T> operation) {
        return withClientSession(clientSession -> {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            MongoEntityOperation<T> op = createMongoDeleteOneOperation(ctx, persistentEntity, operation.getEntity());
            op.delete();
            return (int) op.modifiedCount;
        });
    }

    @Override
    public <T> Optional<Number> deleteAll(DeleteBatchOperation<T> operation) {
        return withClientSession(clientSession -> {
            RuntimePersistentEntity<T> persistentEntity = runtimeEntityRegistry.getEntity(operation.getRootEntity());
            if (operation.all()) {
                MongoDatabase mongoDatabase = getDatabase(persistentEntity, operation.getRepositoryType());
                long deletedCount = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType()).deleteMany(EMPTY).getDeletedCount();
                return Optional.of(deletedCount);
            }
            MongoOperationContext ctx = new MongoOperationContext(clientSession, operation.getAnnotationMetadata(), operation.getRepositoryType());
            MongoEntitiesOperation<T> op = createMongoDeleteManyOperation(ctx, persistentEntity, operation);
            op.delete();
            return Optional.of(op.modifiedCount);
        });
    }

    @Override
    public Optional<Number> executeUpdate(PreparedQuery<?, Number> preparedQuery) {
        return withClientSession(clientSession -> {
            MongoPreparedQuery<?, Number, MongoDatabase> mongoPreparedQuery = getMongoPreparedQuery(preparedQuery);
            RuntimePersistentEntity<?> persistentEntity = mongoPreparedQuery.getRuntimePersistentEntity();
            MongoDatabase database = mongoPreparedQuery.getDatabase();
            Bson filter = mongoPreparedQuery.getFilterOrEmpty();
            Bson update = mongoPreparedQuery.getRequiredUpdate();
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'updateMany' with filter: {} and update: {}", filter.toBsonDocument().toJson(), update.toBsonDocument().toJson());
            }
            UpdateResult updateResult = getCollection(database, persistentEntity, persistentEntity.getIntrospection().getBeanType())
                    .updateMany(clientSession, filter, update);
            if (preparedQuery.isOptimisticLock()) {
                checkOptimisticLocking(1, (int) updateResult.getModifiedCount());
            }
            return Optional.of(updateResult.getModifiedCount());
        });
    }

    @Override
    public Optional<Number> executeDelete(PreparedQuery<?, Number> preparedQuery) {
        return withClientSession(clientSession -> {
            MongoPreparedQuery<?, Number, MongoDatabase> mongoPreparedQuery = getMongoPreparedQuery(preparedQuery);
            RuntimePersistentEntity<?> persistentEntity = mongoPreparedQuery.getRuntimePersistentEntity();
            MongoDatabase mongoDatabase = mongoPreparedQuery.getDatabase();
            Bson filter = mongoPreparedQuery.getFilterOrEmpty();
            if (QUERY_LOG.isDebugEnabled()) {
                QUERY_LOG.debug("Executing Mongo 'deleteMany' with filter: {}", filter.toBsonDocument().toJson());
            }
            DeleteResult deleteResult = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType()).
                    deleteMany(clientSession, filter);
            if (preparedQuery.isOptimisticLock()) {
                checkOptimisticLocking(1, (int) deleteResult.getDeletedCount());
            }
            return Optional.of(deleteResult.getDeletedCount());
        });
    }

    private <K> K triggerPostLoad(AnnotationMetadata annotationMetadata, RuntimePersistentEntity<K> persistentEntity, K entity) {
        if (persistentEntity.hasPostLoadEventListeners()) {
            entity = triggerPostLoad(entity, persistentEntity, annotationMetadata);
        }
        for (PersistentProperty pp : persistentEntity.getPersistentProperties()) {
            if (pp instanceof RuntimeAssociation) {
                RuntimeAssociation runtimeAssociation = (RuntimeAssociation) pp;
                Object o = runtimeAssociation.getProperty().get(entity);
                if (o == null) {
                    continue;
                }
                RuntimePersistentEntity associatedEntity = runtimeAssociation.getAssociatedEntity();
                switch (runtimeAssociation.getKind()) {
                    case MANY_TO_MANY:
                    case ONE_TO_MANY:
                        if (o instanceof Iterable) {
                            for (Object value : ((Iterable) o)) {
                                triggerPostLoad(value, associatedEntity, annotationMetadata);
                            }
                        }
                        continue;
                    case MANY_TO_ONE:
                    case ONE_TO_ONE:
                    case EMBEDDED:
                        triggerPostLoad(o, associatedEntity, annotationMetadata);
                        continue;
                    default:
                        throw new IllegalStateException("Unknown kind: " + runtimeAssociation.getKind());
                }
            }
        }
        return entity;
    }

    @Override
    public void setStatementParameter(Object preparedStatement, int index, DataType dataType, Object value, Dialect dialect) {

    }

    private <T, R> MongoCollection<R> getCollection(MongoDatabase database, RuntimePersistentEntity<T> persistentEntity, Class<R> resultType) {
        return database.getCollection(persistentEntity.getPersistedName(), resultType);
    }

    @Override
    protected MongoDatabase getDatabase(RuntimePersistentEntity<?> persistentEntity, Class<?> repositoryClass) {
        if (repositoryClass != null) {
            String database = repoDatabaseConfig.get(repositoryClass);
            if (database != null) {
                return mongoClient.getDatabase(database);
            }
        }
        return mongoDatabaseFactory.getDatabase(persistentEntity);
    }

    @Override
    protected CodecRegistry getCodecRegistry(MongoDatabase mongoDatabase) {
        return mongoDatabase.getCodecRegistry();
    }

    @Override
    public <T> T persistOne(MongoOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoEntityOperation<T> op = createMongoInsertOneOperation(ctx, persistentEntity, value);
        op.persist();
        return op.getEntity();
    }

    @Override
    public <T> List<T> persistBatch(MongoOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity, Predicate<T> predicate) {
        MongoEntitiesOperation<T> op = createMongoInsertManyOperation(ctx, persistentEntity, values);
        if (predicate != null) {
            op.veto(predicate);
        }
        op.persist();
        return op.getEntities();
    }

    @Override
    public <T> T updateOne(MongoOperationContext ctx, T value, RuntimePersistentEntity<T> persistentEntity) {
        MongoEntityOperation<T> op = createMongoReplaceOneOperation(ctx, persistentEntity, value);
        op.update();
        return op.getEntity();
    }

    private <T> List<T> updateBatch(MongoOperationContext ctx, Iterable<T> values, RuntimePersistentEntity<T> persistentEntity) {
        MongoEntitiesOperation<T> op = createMongoReplaceManyOperation(ctx, persistentEntity, values);
        op.update();
        return op.getEntities();
    }

    @Override
    public void persistManyAssociation(MongoOperationContext ctx,
                                       RuntimeAssociation runtimeAssociation,
                                       Object value,
                                       RuntimePersistentEntity<Object> persistentEntity,
                                       Object child,
                                       RuntimePersistentEntity<Object> childPersistentEntity) {
        String joinCollectionName = runtimeAssociation.getOwner().getNamingStrategy().mappedName(runtimeAssociation);
        MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
        MongoCollection<BsonDocument> collection = mongoDatabase.getCollection(joinCollectionName, BsonDocument.class);
        BsonDocument association = association(collection.getCodecRegistry(), value, persistentEntity, child, childPersistentEntity);
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'insertOne' for collection: {} with document: {}", collection.getNamespace().getFullName(), association);
        }
        collection.insertOne(ctx.clientSession, association);
    }

    @Override
    public void persistManyAssociationBatch(MongoOperationContext ctx, RuntimeAssociation runtimeAssociation,
                                            Object value,
                                            RuntimePersistentEntity<Object> persistentEntity,
                                            Iterable<Object> child,
                                            RuntimePersistentEntity<Object> childPersistentEntity) {
        String joinCollectionName = runtimeAssociation.getOwner().getNamingStrategy().mappedName(runtimeAssociation);
        MongoCollection<BsonDocument> collection = getDatabase(persistentEntity, ctx.repositoryType).getCollection(joinCollectionName, BsonDocument.class);
        List<BsonDocument> associations = new ArrayList<>();
        for (Object c : child) {
            associations.add(association(collection.getCodecRegistry(), value, persistentEntity, c, childPersistentEntity));
        }
        if (QUERY_LOG.isDebugEnabled()) {
            QUERY_LOG.debug("Executing Mongo 'insertMany' for collection: {} with documents: {}", collection.getNamespace().getFullName(), associations);
        }
        collection.insertMany(ctx.clientSession, associations);
    }

    private <T> T withClientSession(Function<ClientSession, T> function) {
        ClientSession clientSession = transactionManager.findClientSession();
        if (clientSession != null) {
            return function.apply(clientSession);
        }
        try (ClientSession cs = mongoClient.startSession()) {
            return function.apply(cs);
        }
    }

    private <T> MongoEntityOperation<T> createMongoInsertOneOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoEntityOperation<T>(ctx, persistentEntity, entity, true) {

            @Override
            protected void execute() throws RuntimeException {
                MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
                MongoCollection<T> collection = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType());
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertOne' with entity: {}", entity);
                }
                InsertOneResult insertOneResult = collection.insertOne(ctx.clientSession, entity);
                BsonValue insertedId = insertOneResult.getInsertedId();
                BeanProperty<T, Object> property = (BeanProperty<T, Object>) persistentEntity.getIdentity().getProperty();
                if (property.get(entity) == null) {
                    entity = updateEntityId(property, entity, insertedId);
                }
            }
        };
    }

    private <T> MongoEntityOperation<T> createMongoReplaceOneOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoEntityOperation<T>(ctx, persistentEntity, entity, false) {

            final MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
            final MongoCollection<BsonDocument> collection = getCollection(mongoDatabase, persistentEntity, BsonDocument.class);
            Bson filter;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filter = MongoUtils.filterByIdAndVersion(conversionService, persistentEntity, entity, collection.getCodecRegistry());
            }

            @Override
            protected void execute() throws RuntimeException {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                BsonDocument bsonDocument = BsonDocumentWrapper.asBsonDocument(entity, mongoDatabase.getCodecRegistry());
                bsonDocument.remove("_id");
                UpdateResult updateResult = collection.replaceOne(ctx.clientSession, filter, bsonDocument);
                modifiedCount = updateResult.getModifiedCount();
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(1, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoEntitiesOperation<T> createMongoReplaceManyOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            final MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
            final MongoCollection<BsonDocument> collection = getCollection(mongoDatabase, persistentEntity, BsonDocument.class);
            Map<Data, Bson> filters;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filters = entities.stream()
                        .collect(Collectors.toMap(d -> d, d -> MongoUtils.filterByIdAndVersion(conversionService, persistentEntity, d.entity, collection.getCodecRegistry())));
            }

            @Override
            protected void execute() throws RuntimeException {
                int expectedToBeUpdated = 0;
                for (Data d : entities) {
                    if (d.vetoed) {
                        continue;
                    }
                    expectedToBeUpdated++;
                    Bson filter = filters.get(d);
                    if (QUERY_LOG.isDebugEnabled()) {
                        QUERY_LOG.debug("Executing Mongo 'replaceOne' with filter: {}", filter.toBsonDocument().toJson());
                    }
                    BsonDocument bsonDocument = BsonDocumentWrapper.asBsonDocument(d.entity, mongoDatabase.getCodecRegistry());
                    bsonDocument.remove("_id");
                    UpdateResult updateResult = collection.replaceOne(ctx.clientSession, filter, bsonDocument);
                    modifiedCount += updateResult.getModifiedCount();
                }
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(expectedToBeUpdated, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoEntityOperation<T> createMongoDeleteOneOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity) {
        return new MongoEntityOperation<T>(ctx, persistentEntity, entity, false) {

            final MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
            final MongoCollection<T> collection = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType());
            Bson filter;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filter = MongoUtils.filterByIdAndVersion(conversionService, persistentEntity, entity, collection.getCodecRegistry());
            }

            @Override
            protected void execute() throws RuntimeException {
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'deleteOne' with filter: {}", filter.toBsonDocument().toJson());
                }
                DeleteResult deleteResult = collection.deleteOne(ctx.clientSession, filter);
                modifiedCount = deleteResult.getDeletedCount();
                if (persistentEntity.getVersion() != null) {
                    checkOptimisticLocking(1, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoEntitiesOperation<T> createMongoDeleteManyOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoEntitiesOperation<T>(ctx, persistentEntity, entities, false) {

            final MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
            final MongoCollection<T> collection = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType());
            Map<Data, Bson> filters;

            @Override
            protected void collectAutoPopulatedPreviousValues() {
                filters = entities.stream().collect(Collectors.toMap(d -> d, d -> MongoUtils.filterByIdAndVersion(conversionService, persistentEntity, d.entity, collection.getCodecRegistry())));
            }

            @Override
            protected void execute() throws RuntimeException {
                List<Bson> filters = entities.stream().filter(d -> !d.vetoed).map(d -> this.filters.get(d)).collect(Collectors.toList());
                if (!filters.isEmpty()) {
                    Bson filter = Filters.or(filters);
                    if (QUERY_LOG.isDebugEnabled()) {
                        QUERY_LOG.debug("Executing Mongo 'deleteMany' with filter: {}", filter.toBsonDocument().toJson());
                    }
                    DeleteResult deleteResult = collection.deleteMany(ctx.clientSession, filter);
                    modifiedCount = deleteResult.getDeletedCount();
                }
                if (persistentEntity.getVersion() != null) {
                    int expected = (int) entities.stream().filter(d -> !d.vetoed).count();
                    checkOptimisticLocking(expected, (int) modifiedCount);
                }
            }
        };
    }

    private <T> MongoEntitiesOperation<T> createMongoInsertManyOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities) {
        return new MongoEntitiesOperation<T>(ctx, persistentEntity, entities, true) {

            @Override
            protected void execute() throws RuntimeException {
                List<T> toInsert = entities.stream().filter(d -> !d.vetoed).map(d -> d.entity).collect(Collectors.toList());
                if (toInsert.isEmpty()) {
                    return;
                }
                if (QUERY_LOG.isDebugEnabled()) {
                    QUERY_LOG.debug("Executing Mongo 'insertMany' with entities: {}", toInsert);
                }
                MongoDatabase mongoDatabase = getDatabase(persistentEntity, ctx.repositoryType);
                InsertManyResult insertManyResult = getCollection(mongoDatabase, persistentEntity, persistentEntity.getIntrospection().getBeanType())
                        .insertMany(ctx.clientSession, toInsert);
                if (hasGeneratedId) {
                    Map<Integer, BsonValue> insertedIds = insertManyResult.getInsertedIds();
                    RuntimePersistentProperty<T> identity = persistentEntity.getIdentity();
                    BeanProperty<T, Object> idProperty = (BeanProperty<T, Object>) identity.getProperty();
                    int index = 0;
                    for (Data d : entities) {
                        if (!d.vetoed) {
                            BsonValue id = insertedIds.get(index);
                            if (id == null) {
                                throw new DataAccessException("Failed to generate ID for entity: " + d.entity);
                            }
                            d.entity = updateEntityId(idProperty, d.entity, id);
                        }
                        index++;
                    }
                }
            }
        };
    }

    @NonNull
    @Override
    public ExecutorAsyncOperations async() {
        ExecutorAsyncOperations asyncOperations = this.asyncOperations;
        if (asyncOperations == null) {
            synchronized (this) { // double check
                asyncOperations = this.asyncOperations;
                if (asyncOperations == null) {
                    asyncOperations = new ExecutorAsyncOperations(
                            this,
                            executorService != null ? executorService : newLocalThreadPool()
                    );
                    this.asyncOperations = asyncOperations;
                }
            }
        }
        return asyncOperations;
    }

    @NonNull
    private ExecutorService newLocalThreadPool() {
        this.executorService = Executors.newCachedThreadPool();
        return executorService;
    }

    @NonNull
    @Override
    public ReactiveRepositoryOperations reactive() {
        return new ExecutorReactiveOperations(async(), conversionService);
    }

    private abstract class MongoEntityOperation<T> extends AbstractSyncEntityOperations<MongoOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        /**
         * Create a new instance.
         *
         * @param ctx              The context
         * @param persistentEntity The RuntimePersistentEntity
         * @param entity           The entity instance
         * @param insert           Is insert operation
         */
        protected MongoEntityOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, T entity, boolean insert) {
            super(ctx, DefaultMongoRepositoryOperations.this.cascadeOperations, DefaultMongoRepositoryOperations.this.entityEventRegistry, persistentEntity, DefaultMongoRepositoryOperations.this.conversionService, entity, insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }
    }

    private abstract class MongoEntitiesOperation<T> extends AbstractSyncEntitiesOperations<MongoOperationContext, T, RuntimeException> {

        protected long modifiedCount;

        protected MongoEntitiesOperation(MongoOperationContext ctx, RuntimePersistentEntity<T> persistentEntity, Iterable<T> entities, boolean insert) {
            super(ctx, DefaultMongoRepositoryOperations.this.cascadeOperations, DefaultMongoRepositoryOperations.this.conversionService, DefaultMongoRepositoryOperations.this.entityEventRegistry, persistentEntity, entities, insert);
        }

        @Override
        protected void collectAutoPopulatedPreviousValues() {
        }

    }

    protected static class MongoOperationContext extends OperationContext {

        private final ClientSession clientSession;

        public MongoOperationContext(ClientSession clientSession, AnnotationMetadata annotationMetadata, Class<?> repositoryType) {
            super(annotationMetadata, repositoryType);
            this.clientSession = clientSession;
        }
    }
}
