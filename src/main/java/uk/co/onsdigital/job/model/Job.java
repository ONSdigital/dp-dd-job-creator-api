package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Represents a job that has been submitted.
 */
@Data @Builder
public class Job {
    private String id;
    private @NonNull Status status = Status.PENDING;
    private @NonNull List<FileStatus> files = Collections.emptyList();
    private @JsonIgnore @NonNull Instant expiryTime;

    @JsonIgnore
    public boolean isComplete() {
        return status == Status.COMPLETE;
    }
}
