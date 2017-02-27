package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.NonNull;
import uk.co.onsdigital.discovery.model.*;

import java.util.Date;


/**
 * Indicates the statusDto of a particular output file in a job.
 */
public class FileStatusDto {
    private  String name;

    @NonNull
    private StatusDto status = StatusDto.PENDING;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String url;

    private Date submittedAt;

    public FileStatusDto(String name) {
        this.name = name;
    }

    @JsonIgnore
    public boolean isComplete() {
        return status == StatusDto.COMPLETE;
    }

    /** Whether the job has been submitted to the filterer or not. */
    @JsonIgnore
    public boolean isSubmitted() {
        return submittedAt != null;
    }

    @JsonIgnore
    public Date getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(Date submittedAt) {
        this.submittedAt = submittedAt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public StatusDto getStatus() {
        return status;
    }

    public void setStatus(StatusDto status) {
        this.status = status;
    }

    public FileStatus convertToModel() {
        FileStatus fileStatus = new FileStatus();
        fileStatus.setStatus(StatusDto.convertToModel(this.status));
        fileStatus.setName(this.name);
        fileStatus.setUrl(this.url);
        fileStatus.setSubmittedAt(this.submittedAt);
        return fileStatus;
    }

    public static FileStatusDto convertFromModel(FileStatus fileStatus) {
        FileStatusDto fileStatusDto = new FileStatusDto(fileStatus.getName());
        fileStatusDto.setStatus(StatusDto.fromString(fileStatus.getStatus().toString()));
        fileStatusDto.setUrl(fileStatus.getUrl());
        fileStatusDto.setSubmittedAt(fileStatus.getSubmittedAt());

        return fileStatusDto;
    }

    @Override
    public String toString() {
        return "FileStatusDto{name='" + name  + "', status=" + status + ", url='" + url + "', submittedAt=" + submittedAt + "}";
    }
}
