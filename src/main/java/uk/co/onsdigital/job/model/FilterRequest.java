package uk.co.onsdigital.job.model;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;
import java.util.Set;

/**
 * Request to be sent to the CSV filter to create output files.
 */
@Data @Builder
public class FilterRequest {
    private @NonNull String requestId;
    private @NonNull String inputUrl;
    private @NonNull String outputUrl;
    private @NonNull Map<String, ? extends Set<String>> dimensions;
}
