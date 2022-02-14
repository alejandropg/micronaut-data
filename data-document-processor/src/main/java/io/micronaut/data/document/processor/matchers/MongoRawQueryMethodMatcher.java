/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.data.document.processor.matchers;

import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.Query;
import io.micronaut.data.document.mongo.MongoAnnotations;
import io.micronaut.data.intercept.DataInterceptor;
import io.micronaut.data.model.PersistentPropertyPath;
import io.micronaut.data.model.query.BindingParameter.BindingContext;
import io.micronaut.data.model.query.builder.QueryParameterBinding;
import io.micronaut.data.model.query.builder.QueryResult;
import io.micronaut.data.processor.model.SourcePersistentEntity;
import io.micronaut.data.processor.model.criteria.impl.SourceParameterExpressionImpl;
import io.micronaut.data.processor.visitors.MatchFailedException;
import io.micronaut.data.processor.visitors.MethodMatchContext;
import io.micronaut.data.processor.visitors.finders.FindersUtils;
import io.micronaut.data.processor.visitors.finders.MethodMatchInfo;
import io.micronaut.data.processor.visitors.finders.MethodMatcher;
import io.micronaut.inject.ast.ClassElement;
import io.micronaut.inject.ast.MethodElement;
import io.micronaut.inject.ast.ParameterElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Finder with custom defied query used to return a single result.
 *
 * @author Denis Stepanov
 * @since 3.3.0
 */
public class MongoRawQueryMethodMatcher implements MethodMatcher {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("([^:]*)((?<![:]):([a-zA-Z]+[a-zA-Z0-9]+))([^:]*)");

    /**
     * Default constructor.
     */
    public MongoRawQueryMethodMatcher() {
    }

    @Override
    public final int getOrder() {
        // should run first and before `RawQueryMethodMatcher`
        return DEFAULT_POSITION - 2000;
    }

    @Override
    public MethodMatch match(MethodMatchContext matchContext) {
        if (!matchContext.getAnnotationMetadata().hasAnnotation(MongoAnnotations.REPOSITORY)) {
            return null;
        }
        if (matchContext.getMethodElement().stringValue(Query.class).isPresent()) {
            throw new MatchFailedException("`@Query` annotations is not supported for MongoDB repositories. Use one of the annotations from `io.micronaut.data.mongodb.annotation` for a custom query.");
        }
        if (matchContext.getMethodElement().hasAnnotation(MongoAnnotations.FIND_QUERY)) {
            return new MethodMatch() {

                @Override
                public MethodMatchInfo buildMatchInfo(MethodMatchContext matchContext) {
                    MethodMatchInfo.OperationType operationType = MethodMatchInfo.OperationType.QUERY;

                    Map.Entry<ClassElement, Class<? extends DataInterceptor>> entry = FindersUtils.resolveInterceptorTypeByOperationType(
                            false,
                            false,
                            operationType,
                            matchContext);

                    ClassElement resultType = entry.getKey();
                    Class<? extends DataInterceptor> interceptorType = entry.getValue();

                    boolean isDto = false;
                    if (resultType == null) {
                        resultType = matchContext.getRootEntity().getType();
                    } else {
                        if (resultType.hasAnnotation(Introspected.class)) {
                            if (!resultType.hasAnnotation(MappedEntity.class)) {
                                isDto = true;
                            }
                        }
                    }

                    MethodMatchInfo methodMatchInfo = new MethodMatchInfo(
                            resultType,
                            FindersUtils.getInterceptorElement(matchContext, interceptorType)
                    );

                    methodMatchInfo.dto(isDto);

                    buildRawQuery(matchContext, methodMatchInfo, null, null, operationType);

                    return methodMatchInfo;
                }
            };
        }
        return null;
    }

    private void buildRawQuery(@NonNull MethodMatchContext matchContext,
                               MethodMatchInfo methodMatchInfo,
                               ParameterElement entityParameter,
                               ParameterElement entitiesParameter,
                               MethodMatchInfo.OperationType operationType) {
        MethodElement methodElement = matchContext.getMethodElement();
        String queryString = methodElement.stringValue(MongoAnnotations.FIND_QUERY).orElseThrow(() ->
                new IllegalStateException("Should only be called if Query has value!")
        );
        List<ParameterElement> parameters = Arrays.asList(matchContext.getParameters());
        ParameterElement entityParam = null;
        SourcePersistentEntity persistentEntity = null;
        if (entityParameter != null) {
            entityParam = entityParameter;
            persistentEntity = matchContext.getEntity(entityParameter.getGenericType());
        } else if (entitiesParameter != null) {
            entityParam = entitiesParameter;
            persistentEntity = matchContext.getEntity(entitiesParameter.getGenericType().getFirstTypeArgument().get());
        }

        QueryResult queryResult = getQueryResult(matchContext, queryString, parameters, entityParam, persistentEntity);
//        String cq = matchContext.getAnnotationMetadata().stringValue(Query.class, "countQuery")
//                .orElse(null);
//        QueryResult countQueryResult = cq == null ? null : getQueryResult(matchContext, cq, parameters, entityParam, persistentEntity);
        boolean encodeEntityParameters = persistentEntity != null || operationType == MethodMatchInfo.OperationType.INSERT;

        methodElement.annotate(Query.class, builder -> builder.value(queryResult.getQuery()));

        methodMatchInfo
                .isRawQuery(true)
                .encodeEntityParameters(encodeEntityParameters)
                .queryResult(queryResult)
                .countQueryResult(null);
    }

    private QueryResult getQueryResult(MethodMatchContext matchContext,
                                       String queryString,
                                       List<ParameterElement> parameters,
                                       ParameterElement entityParam,
                                       SourcePersistentEntity persistentEntity) {
        java.util.regex.Matcher matcher = VARIABLE_PATTERN.matcher(queryString);
        List<QueryParameterBinding> parameterBindings = new ArrayList<>(parameters.size());
        List<String> queryParts = new ArrayList<>();
        boolean requiresEnd = true;
        while (matcher.find()) {
            requiresEnd = true;
            String start = queryString.substring(0, matcher.start(3) - 1);
            if (!start.isEmpty()) {
                queryParts.add(start);
            }

            String name = matcher.group(3);

            Optional<ParameterElement> element = parameters.stream()
                    .filter(p -> p.stringValue(Parameter.class).orElse(p.getName()).equals(name))
                    .findFirst();
            if (element.isPresent()) {
                PersistentPropertyPath propertyPath = matchContext.getRootEntity().getPropertyPath(name);
                BindingContext bindingContext = BindingContext.create()
                        .name(name)
                        .incomingMethodParameterProperty(propertyPath)
                        .outgoingQueryParameterProperty(propertyPath);
                parameterBindings.add(bindingParameter(matchContext, element.get()).bind(bindingContext));
            } else if (persistentEntity != null) {
                PersistentPropertyPath propertyPath = persistentEntity.getPropertyPath(name);
                if (propertyPath == null) {
                    throw new MatchFailedException("Cannot update non-existent property: " + name);
                } else {
                    BindingContext bindingContext = BindingContext.create()
                            .name(name)
                            .incomingMethodParameterProperty(propertyPath)
                            .outgoingQueryParameterProperty(propertyPath);
                    parameterBindings.add(bindingParameter(matchContext, entityParam, true).bind(bindingContext));
                }
            } else {
                throw new MatchFailedException("No method parameter found for named Query parameter: " + name);
            }

            int ind = parameterBindings.size() - 1;
            queryParts.add("{$mn_qp:" + ind + "}");
            String end = queryString.substring(matcher.end(3));
            if (!end.isEmpty()) {
                requiresEnd = false;
                queryParts.add(end);
            }
        }
        if (queryParts.isEmpty()) {
            queryParts.add(queryString);
        } else if (requiresEnd) {
            queryParts.add("");
        }
        String query = queryParts.stream().collect(Collectors.joining());
        return new QueryResult() {
            @Override
            public String getQuery() {
                return query;
            }

            @Override
            public List<String> getQueryParts() {
                return queryParts;
            }

            @Override
            public List<QueryParameterBinding> getParameterBindings() {
                return parameterBindings;
            }

            @Override
            public Map<String, String> getAdditionalRequiredParameters() {
                return Collections.emptyMap();
            }
        };
    }

    private SourceParameterExpressionImpl bindingParameter(MethodMatchContext matchContext, ParameterElement element) {
        return bindingParameter(matchContext, element, false);
    }

    private SourceParameterExpressionImpl bindingParameter(MethodMatchContext matchContext, ParameterElement element, boolean isEntityParameter) {
        return new SourceParameterExpressionImpl(
                Collections.emptyMap(),
                matchContext.getParameters(),
                element,
                isEntityParameter);
    }

}
