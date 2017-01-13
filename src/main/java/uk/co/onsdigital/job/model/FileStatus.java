package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NonNull;

/**
 * Indicates the status of a particular output file in a job.
 */
@Data
public class FileStatus {
    private @NonNull String name;
    private @NonNull Status status = Status.PENDING;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String url;

    @JsonIgnore
    public boolean isComplete() {
        return status == Status.COMPLETE;
    }
}
