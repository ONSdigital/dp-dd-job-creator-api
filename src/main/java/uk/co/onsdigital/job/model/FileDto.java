package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.NonNull;
import uk.co.onsdigital.discovery.model.*;

import java.util.Date;


/**
 * Indicates the statusDto of a particular output file in a job.
 */
public class FileDto {
    private  String name;

    @NonNull
    private StatusDto status = StatusDto.PENDING;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String url;

    private Date submittedAt;

    public FileDto(String name) {
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

    public File convertToModel() {
        File file = new File();
        file.setStatus(StatusDto.convertToModel(this.status));
        file.setName(this.name);
        file.setUrl(this.url);
        file.setSubmittedAt(this.submittedAt);
        return file;
    }

    public static FileDto convertFromModel(File file) {
        FileDto fileDto = new FileDto(file.getName());
        fileDto.setStatus(StatusDto.fromString(file.getStatus().toString()));
        fileDto.setUrl(file.getUrl());
        fileDto.setSubmittedAt(file.getSubmittedAt());

        return fileDto;
    }

    @Override
    public String toString() {
        return "FileDto{name='" + name  + "', status=" + status + ", url='" + url + "', submittedAt=" + submittedAt + "}";
    }
}
