package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
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

    /**
     * Returns the dimensions as a sorted map with all options also sorted. This provides a deterministic order for all
     * dimensions allowing to detect duplicate jobs.
     *
     * @return the sorted dimensions from the request.
     */
    public SortedMap<String, SortedSet<String>> getSortedDimensionFilters() {
        final SortedMap<String, SortedSet<String>> result = new TreeMap<>();
        for (DimensionFilter filter : dimensions) {
            result.put(filter.getId(), new TreeSet<>(filter.getOptions()));
        }
        return result;
    }
}
