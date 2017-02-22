package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Tolerate;
import uk.co.onsdigital.discovery.model.FileStatus;
import uk.co.onsdigital.discovery.model.Status;


/**
 * Indicates the statusDto of a particular output file in a job.
 */
public class FileStatusDto {
    private  String name;

    @NonNull
    private StatusDto status = StatusDto.PENDING;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String url;

    @Tolerate
    FileStatusDto() {
        // Default constructor for JPA
    }

    public FileStatusDto(String name) {
        this.name = name;
    }

    @JsonIgnore
    public boolean isComplete() {
        return status == StatusDto.COMPLETE;
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
        return fileStatus;
    }

    public static FileStatusDto convertFromModel(FileStatus fileStatus) {
        FileStatusDto fileStatusDto = new FileStatusDto();

        fileStatusDto.setName(fileStatus.getName());
        fileStatusDto.setStatus(StatusDto.fromString(fileStatus.getStatus().toString()));
        fileStatusDto.setUrl(fileStatus.getUrl());

        return fileStatusDto;
    }

}
