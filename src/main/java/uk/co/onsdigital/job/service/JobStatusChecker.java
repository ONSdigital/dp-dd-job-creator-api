package uk.co.onsdigital.job.service;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriTemplate;
import uk.co.onsdigital.job.model.FileDto;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

/**
 * Service for checking the status of file creation jobs.
 */
@Service
public class JobStatusChecker {
    private static final Logger log = LoggerFactory.getLogger(JobStatusChecker.class);

    private final AmazonS3 s3Client;
    private final String outputS3Bucket;
    private final UriTemplate downloadUrlTemplate;

    @Autowired
    JobStatusChecker(final AmazonS3 s3Client,
                     final @Value("${output.s3.bucket}") String outputS3Bucket,
                     final @Value("${download.url.template}") UriTemplate downloadUrlTemplate) {
        log.info("Starting JobStatusChecker. s3.bucket={}, url.template={}", outputS3Bucket, downloadUrlTemplate);

        this.s3Client = s3Client;
        this.outputS3Bucket = outputS3Bucket;
        this.downloadUrlTemplate = downloadUrlTemplate;
    }

    /**
     * Updates the status of the given jobDto by polling S3 to see if the output files have been created.
     *
     * @param jobDto the jobDto to check and update the status of.
     */
    public void updateStatus(JobDto jobDto) {
        log.debug("Checking status of job: {}", jobDto);
        if (!jobDto.isComplete()) {
            for (FileDto fileDto : jobDto.getFiles()) {
                if (!fileDto.isComplete()) {
                    if (s3Client.doesObjectExist(outputS3Bucket, fileDto.getName())) {
                        fileDto.setStatus(StatusDto.COMPLETE);
                        fileDto.setUrl(downloadUrlTemplate.expand(fileDto.getName()).toString());
                    }
                }
            }
            if (jobDto.getFiles().stream().allMatch(FileDto::isComplete)) {
                jobDto.setStatus(StatusDto.COMPLETE);
            }
        }
        log.debug("Checked status of job: {}", jobDto);
    }
}
