package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A request to create a new job. The request will be JSON in the following format:
 * <pre>{@code
 *  {
 *      "id" : "the dataset uuid",
 *      "dimensions" : [ {"id" : "dimension id", "options" : ["dimension", "values"]}],
 *      "fileFormats" : ["CSV"]
 *  }
 * }</pre>
 */
@Data
public class CreateJobRequest {
    @JsonProperty("id")
    private UUID dataSetId;
    private List<DimensionFilter> dimensions = Collections.emptyList();
    private Set<FileFormat> fileFormats = Collections.singleton(FileFormat.CSV);
}
