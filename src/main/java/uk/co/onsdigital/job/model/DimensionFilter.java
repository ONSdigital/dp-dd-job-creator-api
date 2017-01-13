package uk.co.onsdigital.job.model;

import lombok.Data;
import lombok.NonNull;

import java.util.List;

/**
 * Represents a dimension in a {@link CreateJobRequest}.
 * <pre>{@code
 *      { "id" : "dimension id", "options" : ["dimension", "values"] }
 * }</pre>
 */
@Data
public class DimensionFilter {
    private @NonNull String id;
    private @NonNull List<String> options;
}
