package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.co.onsdigital.discovery.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Represents a file generation job that has been submitted.
 */
public class JobDto {

    private String id;

    private StatusDto status = StatusDto.PENDING;

    private  List<FileDto> files = Collections.emptyList();

    @JsonIgnore
    private Date expiryTime;

    public JobDto () {

    }

    public JobDto(Collection<FileDto> files, Date expiryTime) {
        this(UUID.randomUUID().toString(), StatusDto.PENDING, new ArrayList<>(files), expiryTime);
    }

    private JobDto(String id, StatusDto status, List<FileDto> files, Date expiryTime) {
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

    public List<FileDto> getFiles() {
        return files;
    }

    public void setFiles(List<FileDto> files) {
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
        List<File> files = this.files.stream().map(FileDto::convertToModel).collect(Collectors.toList());
        job.setFiles(files);
        job.setId(this.id);
        return job;
    }

    public static JobDto convertFromModel(Job job) {
        JobDto jobDto = new JobDto();
        jobDto.setStatus(StatusDto.valueOf(job.getStatus().toString()));
        jobDto.setExpiryTime(job.getExpiryTime());
        jobDto.setId(job.getId());
        jobDto.setFiles(job.getFiles().stream().map(FileDto::convertFromModel).collect(Collectors.toList()));
        return jobDto;
    }

    @Override
    public String toString() {
        return "JobDto{id='" + id + "', status=" + status + ", files=" + files + ", expiryTime=" + expiryTime + '}';
    }
}
