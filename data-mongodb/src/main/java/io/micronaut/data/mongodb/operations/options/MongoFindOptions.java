package io.micronaut.data.mongodb.operations.options;

import com.mongodb.CursorType;
import com.mongodb.client.model.Collation;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.Nullable;
import org.bson.conversions.Bson;

@Experimental
public class MongoFindOptions {
    @Nullable
    private final Bson filter;
    @Nullable
    private final Integer batchSize;
    @Nullable
    private final Integer limit;
    @Nullable
    private final Bson projection;
    @Nullable
    private final Long maxTimeMS;
    @Nullable
    private final Long maxAwaitTimeMS;
    @Nullable
    private final Integer skip;
    @Nullable
    private final Bson sort;
    @Nullable
    private final CursorType cursorType;
    @Nullable
    private final Boolean noCursorTimeout;
    @Nullable
    private final Boolean partial;
    @Nullable
    private final Collation collation;
    @Nullable
    private final String comment;
    @Nullable
    private final Bson hint;
    @Nullable
    private final Bson max;
    @Nullable
    private final Bson min;
    @Nullable
    private final Boolean returnKey;
    @Nullable
    private final Boolean showRecordId;
    @Nullable
    private final Boolean allowDiskUse;

    public MongoFindOptions(Bson filter, Integer batchSize, Integer limit, Bson projection, Long maxTimeMS, Long maxAwaitTimeMS, Integer skip,
                            Bson sort, CursorType cursorType, Boolean noCursorTimeout, Boolean partial,
                            Collation collation, String comment, Bson hint, Bson max, Bson min, Boolean returnKey,
                            Boolean showRecordId, Boolean allowDiskUse) {
        this.filter = filter;
        this.batchSize = batchSize;
        this.limit = limit;
        this.projection = projection;
        this.maxTimeMS = maxTimeMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.skip = skip;
        this.sort = sort;
        this.cursorType = cursorType;
        this.noCursorTimeout = noCursorTimeout;
        this.partial = partial;
        this.collation = collation;
        this.comment = comment;
        this.hint = hint;
        this.max = max;
        this.min = min;
        this.returnKey = returnKey;
        this.showRecordId = showRecordId;
        this.allowDiskUse = allowDiskUse;
    }

    public Bson getFilter() {
        return filter;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getLimit() {
        return limit;
    }

    public Bson getProjection() {
        return projection;
    }

    public Long getMaxTimeMS() {
        return maxTimeMS;
    }

    public Long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    public Integer getSkip() {
        return skip;
    }

    public Bson getSort() {
        return sort;
    }

    public CursorType getCursorType() {
        return cursorType;
    }

    public Boolean getNoCursorTimeout() {
        return noCursorTimeout;
    }

    public Boolean getPartial() {
        return partial;
    }

    public Collation getCollation() {
        return collation;
    }

    public String getComment() {
        return comment;
    }

    public Bson getHint() {
        return hint;
    }

    public Bson getMax() {
        return max;
    }

    public Bson getMin() {
        return min;
    }

    public Boolean getReturnKey() {
        return returnKey;
    }

    public Boolean getShowRecordId() {
        return showRecordId;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }
}
