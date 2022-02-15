package io.micronaut.data.mongodb.operations.options;

import com.mongodb.client.model.Collation;
import io.micronaut.core.annotation.Experimental;
import io.micronaut.core.annotation.Nullable;
import org.bson.conversions.Bson;

@Experimental
public class MongoDeleteOptions {

    private final Bson filter;
    @Nullable
    private final Bson hint;
    @Nullable
    private final Collation collation;

    public MongoDeleteOptions(Bson filter, Bson hint, Collation collation) {
        this.filter = filter;
        this.hint = hint;
        this.collation = collation;
    }

    public Bson getFilter() {
        return filter;
    }

    public Bson getHint() {
        return hint;
    }

    public Collation getCollation() {
        return collation;
    }
}
