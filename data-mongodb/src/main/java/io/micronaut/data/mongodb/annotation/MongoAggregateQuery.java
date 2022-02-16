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
package io.micronaut.data.mongodb.annotation;

import io.micronaut.context.annotation.AliasFor;
import io.micronaut.data.annotation.Query;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a custom MongoDB query for aggregating results.
 *
 * @author Denis Stepanov
 * @since 3.3.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
@Inherited
public @interface MongoAggregateQuery {

    /**
     * The custom MongoDB query represented in JSON. The filter is represented as a JSON object '{...}' or pipeline query is a JSON array of stages '[{...}, {...}]'.
     * <p>
     * If the query starts with '[' it's assumed that it's a pipeline query and `aggregate` method should be executed otherwise it's filter query and `find` should be used.
     * NOTE: The actual execution might be the aggregation if there are multiple stages like sort, fileds etc defined.
     *
     * @return The query
     */
    @AliasFor(member = "value", annotation = Query.class)
    String value() default "";

    /**
     * The custom sorting represented in JSON.
     *
     * @return The sort
     */
    @AliasFor(member = "value", annotation = MongoSort.class)
    String sort() default "";

    /**
     * The custom fields projections represented in JSON.
     *
     * @return The fields
     */
    @AliasFor(member = "value", annotation = MongoProjection.class)
    String project() default "";

    /**
     * The custom collation represented in JSON.
     *
     * @return The collation
     */
    @AliasFor(member = "value", annotation = MongoCollation.class)
    String collation() default "";
}
