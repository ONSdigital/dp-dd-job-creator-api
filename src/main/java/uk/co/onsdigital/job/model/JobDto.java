package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.co.onsdigital.discovery.model.FileStatus;
import uk.co.onsdigital.discovery.model.Job;
import uk.co.onsdigital.discovery.model.Status;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a file generation job that has been submitted.
 */
public class JobDto {

    private String id;

    private StatusDto status = StatusDto.PENDING;

    private  List<FileStatusDto> files = Collections.emptyList();

    private @JsonIgnore Date expiryTime;

    public JobDto () {

    }

    private JobDto(String id, StatusDto status, List<FileStatusDto> files, Date expiryTime) {
        this.id = id;
        this.status = status;
        this.files = files;
        this.expiryTime = expiryTime;
    }

    @JsonIgnore
    public boolean isComplete() {
        return status == StatusDto.COMPLETE;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<FileStatusDto> getFiles() {
        return files;
    }

    public void setFiles(List<FileStatusDto> files) {
        this.files = files;
    }

    public Date getExpiryTime() {
        return expiryTime;
    }

    public void setExpiryTime(Date expiryTime) {
        this.expiryTime = expiryTime;
    }

    public StatusDto getStatus() {
        return status;
    }

    public void setStatus(StatusDto status) {
        this.status = status;
    }

    public Job convertToModel() {
        Job job = new Job();
        job.setStatus(StatusDto.convertToModel(this.status));
        job.setExpiryTime(this.expiryTime);
        List<FileStatus> fileStatuses = this.files.stream().map(FileStatusDto::convertToModel).collect(Collectors.toList());
        job.setFiles(fileStatuses);
        job.setId(this.id);
        return job;
    }

    public static JobDto convertFromModel(Job job) {
        JobDto jobDto = new JobDto();
        jobDto.setStatus(StatusDto.valueOf(job.getStatus().toString()));
        jobDto.setExpiryTime(job.getExpiryTime());
        jobDto.setId(job.getId());
        jobDto.setFiles(job.getFiles().stream().map(FileStatusDto::convertFromModel).collect(Collectors.toList()));
        return jobDto;
    }
}
