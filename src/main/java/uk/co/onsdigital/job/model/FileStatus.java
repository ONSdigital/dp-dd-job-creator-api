package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Tolerate;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Indicates the status of a particular output file in a job.
 */
@Data @Entity @Table(name = "file_status")
public class FileStatus {
    @Id
    private @NonNull String name;

    @Enumerated(EnumType.STRING)
    private @NonNull Status status = Status.PENDING;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String url;

    @Tolerate
    FileStatus() {
        // Default constructor for JPA
    }

    @JsonIgnore
    public boolean isComplete() {
        return status == Status.COMPLETE;
    }
}
