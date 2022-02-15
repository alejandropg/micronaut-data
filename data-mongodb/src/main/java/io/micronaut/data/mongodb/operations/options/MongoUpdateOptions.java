package io.micronaut.data.mongodb.operations.options;

import com.mongodb.client.model.Collation;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.Nullable;
import org.bson.conversions.Bson;

import java.util.List;

@Experimental
public class MongoUpdateOptions {
    private final Bson update;
    private final Bson filter;
    @Nullable
    private final Boolean upsert;
    @Nullable
    private final Boolean bypassDocumentValidation;
    @Nullable
    private final Collation collation;
    @Nullable
    private final List<? extends Bson> arrayFilters;
    @Nullable
    private final Bson hint;

    public MongoUpdateOptions(Bson update, Bson filter, Boolean upsert, Boolean bypassDocumentValidation, Collation collation, List<? extends Bson> arrayFilters, Bson hint) {
        this.update = update;
        this.filter = filter;
        this.upsert = upsert;
        this.bypassDocumentValidation = bypassDocumentValidation;
        this.collation = collation;
        this.arrayFilters = arrayFilters;
        this.hint = hint;
    }

    public Bson getUpdate() {
        return update;
    }

    public Bson getFilter() {
        return filter;
    }

    public Boolean getUpsert() {
        return upsert;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public Collation getCollation() {
        return collation;
    }

    public List<? extends Bson> getArrayFilters() {
        return arrayFilters;
    }

    public Bson getHint() {
        return hint;
    }
}
