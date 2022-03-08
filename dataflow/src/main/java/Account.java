import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
     * A class used for parsing JSON web server events
     * Annotated with @DefaultSchema to the allow the use of Beam Schema and <Row> object
     */
    @VisibleForTesting
    @DefaultSchema(JavaFieldSchema.class)
    public class Account implements Serializable {
        int id;
        @javax.annotation.Nullable String name;
        @javax.annotation.Nullable String surname;

    public int getId() {
        return id;
    }

    @Nullable
    public String getName() {
        return name;
    }

    @Nullable
    public String getSurname() {
        return surname;
    }
}

