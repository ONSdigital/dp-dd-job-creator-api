package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.Tolerate;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Represents a file generation job that has been submitted.
 */
@Entity @Table(name = "file_gen_job")
@Data @Builder
public class Job {
    @Id
    @Column(name = "job_id")
    private @NonNull String id;

    @Enumerated(EnumType.STRING)
    private @NonNull Status status = Status.PENDING;

    @OneToMany(cascade = {CascadeType.ALL}, fetch = FetchType.EAGER)
    private @NonNull @Singular  List<FileStatus> files = Collections.emptyList();

    @Temporal(TemporalType.TIMESTAMP)
    private @JsonIgnore @NonNull Date expiryTime;

    @Tolerate
    Job() {
        // No arg constructor for JPA
    }

    @JsonIgnore
    public boolean isComplete() {
        return status == Status.COMPLETE;
    }
}
