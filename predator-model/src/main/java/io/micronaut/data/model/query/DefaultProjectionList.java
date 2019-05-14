/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.data.model.query;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.micronaut.data.model.query.factory.Projections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default implementation of {@link ProjectionList}.
 *
 * @author graemerocher
 * @since 1.0.0
 */
class DefaultProjectionList implements ProjectionList {

    private List<Query.Projection> projections = new ArrayList(3);

    /**
     * The backing list of projections.
     * @return The list of projections
     */
    public List<Query.Projection> getProjectionList() {
        return Collections.unmodifiableList(projections);
    }

    @Override
    public ProjectionList add(@NonNull Query.Projection p) {
        projections.add(p);
        return this;
    }

    @Override
    public ProjectionList id() {
        add(Projections.id());
        return this;
    }

    @Override
    public ProjectionList count() {
        add(Projections.count());
        return this;
    }

    @Override
    public ProjectionList countDistinct(String property) {
        add(Projections.countDistinct(property));
        return this;
    }

    @Override
    public ProjectionList groupProperty(String property) {
        add(Projections.groupProperty(property));
        return this;
    }

    /**
     * Whether the list is empty.
     * @return True if it is empty
     */
    public boolean isEmpty() {
        return projections.isEmpty();
    }

    @Override
    public ProjectionList distinct() {
        add(Projections.distinct());
        return this;
    }

    @Override
    public ProjectionList distinct(String property) {
        add(Projections.distinct(property));
        return this;
    }

    @Override
    public ProjectionList rowCount() {
        return count();
    }

    /**
     * A projection that obtains the value of a property of an entity.
     * @param name The name of the property
     * @return The PropertyProjection instance
     */
    @Override
    public ProjectionList property(String name) {
        add(Projections.property(name));
        return this;
    }

    /**
     * Computes the sum of a property.
     *
     * @param name The name of the property
     * @return The PropertyProjection instance
     */
    @Override
    public ProjectionList sum(String name) {
        add(Projections.sum(name));
        return this;
    }

    /**
     * Computes the min value of a property.
     *
     * @param name The name of the property
     * @return The PropertyProjection instance
     */
    @Override
    public ProjectionList min(String name) {
        add(Projections.min(name));
        return this;
    }

    /**
     * Computes the pageSize value of a property.
     *
     * @param name The name of the property
     * @return The PropertyProjection instance
     */
    @Override
    public ProjectionList max(String name) {
        add(Projections.max(name));
        return this;
    }

    /**
     * Computes the average value of a property.
     *
     * @param name The name of the property
     * @return The PropertyProjection instance
     */
    @Override
    public ProjectionList avg(String name) {
        add(Projections.avg(name));
        return this;
    }
}