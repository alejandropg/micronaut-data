package io.micronaut.data.mongodb.operations.options;

import com.mongodb.client.model.Collation;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.Nullable;
import org.bson.conversions.Bson;

import java.util.List;

@Experimental
public class MongoAggregateOptions {

    private final List<Bson> pipeline;
    @Nullable
    private final Boolean allowDiskUse;
    @Nullable
    private final Long maxTimeMS;
    @Nullable
    private final Long maxAwaitTimeMS;
    @Nullable
    private final Boolean bypassDocumentValidation;
    @Nullable
    private final Collation collation;
    @Nullable
    private final String comment;
    @Nullable
    private final Bson hint;

    public MongoAggregateOptions(List<Bson> pipeline, Boolean allowDiskUse, Long maxTimeMS, Long maxAwaitTimeMS, Boolean bypassDocumentValidation,
                                 Collation collation, String comment, Bson hint) {
        this.pipeline = pipeline;
        this.allowDiskUse = allowDiskUse;
        this.maxTimeMS = maxTimeMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.collation = collation;
        this.comment = comment;
        this.hint = hint;
    }

    public List<Bson> getPipeline() {
        return pipeline;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public Long getMaxTimeMS() {
        return maxTimeMS;
    }

    public Long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
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
}
