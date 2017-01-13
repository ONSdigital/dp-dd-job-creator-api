package uk.co.onsdigital.job.service;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriTemplate;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.model.Status;

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
     * Updates the status of the given job by polling S3 to see if the output files have been created.
     *
     * @param job the job to check and update the status of.
     */
    public void updateStatus(Job job) {
        log.debug("Checking status of job: {}", job);
        if (!job.isComplete()) {
            for (FileStatus fileStatus : job.getFiles()) {
                if (!fileStatus.isComplete()) {
                    if (s3Client.doesObjectExist(outputS3Bucket, fileStatus.getName())) {
                        fileStatus.setStatus(Status.COMPLETE);
                        fileStatus.setUrl(downloadUrlTemplate.expand(fileStatus.getName()).toString());
                    }
                }
            }
            if (job.getFiles().stream().allMatch(FileStatus::isComplete)) {
                job.setStatus(Status.COMPLETE);
            }
        }
    }
}
