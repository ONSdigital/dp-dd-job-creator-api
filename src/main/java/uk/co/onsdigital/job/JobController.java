package uk.co.onsdigital.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import uk.co.onsdigital.job.exception.NoSuchJobException;
import uk.co.onsdigital.job.exception.TooManyRequestsException;
import uk.co.onsdigital.job.model.CreateJobRequest;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.repository.DataSetRepository;
import uk.co.onsdigital.job.repository.JobRepository;
import uk.co.onsdigital.job.service.FilterServiceClient;
import uk.co.onsdigital.job.service.JobStatusChecker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import static java.time.Instant.now;
import static uk.co.onsdigital.job.model.Status.PENDING;

/**
 * REST API implementation.
 */
@RestController
public class JobController {
    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private static final Base64.Encoder filenameEncoder = Base64.getUrlEncoder().withoutPadding();

    private final DataSetRepository dataSetRepository;
    private final FilterServiceClient filterServiceClient;
    private final JobRepository jobRepository;
    private final JobStatusChecker jobStatusChecker;
    private final long pendingJobLimit;

    @Autowired
    JobController(DataSetRepository dataSetRepository, FilterServiceClient filterServiceClient,
                  JobRepository jobRepository, JobStatusChecker jobStatusChecker, @Value("${pending.job.limit}") long pendingJobLimit) {
        this.dataSetRepository = dataSetRepository;
        this.filterServiceClient = filterServiceClient;
        this.jobStatusChecker = jobStatusChecker;
        this.jobRepository = jobRepository;
        this.pendingJobLimit = pendingJobLimit;
        log.info("pendingJobLimit=" + pendingJobLimit);
    }


    @PostMapping(value = "/job", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    @CrossOrigin
    @Transactional
    public Job createJob(final @RequestBody CreateJobRequest request) throws JsonProcessingException {
        log.debug("Processing job request: {}", request);
        final String dataSetS3Url = dataSetRepository.findS3urlForDataSet(request.getDataSetId());
        final Map<FileFormat, FileStatus> files = generateFileNames(request);

        final Job job = Job.builder()
                .id(UUID.randomUUID().toString())
                .status(PENDING)
                .files(files.values())
                .expiryTime(new Date(now().plus(1, ChronoUnit.HOURS).toEpochMilli()))
                .build();

        // Check to see if the files already exist
        jobStatusChecker.updateStatus(job);

        if (job.isComplete()) {
            return jobRepository.save(job);
        }
        if (jobRepository.countJobsWithStatus(PENDING) >= pendingJobLimit) {
            throw new TooManyRequestsException("Sorry - the number of requested jobs exceeds the limit");
        }

        filterServiceClient.submitFilterRequest(dataSetS3Url, files, request.getSortedDimensionFilters());
        return jobRepository.save(job);
    }

    @GetMapping("/job/{id}")
    @ResponseBody
    @CrossOrigin
    @Transactional
    public Job checkJobStatus(final @PathVariable("id") String jobId) {
        log.debug("Checking status for: {}", jobId);
        Job job = jobRepository.getOne(jobId);
        if (job == null) {
            throw new NoSuchJobException(jobId);
        }

        if (job.getExpiryTime().before(new Date())) {
            log.debug("Deleting expired job: {}", job);
            jobRepository.delete(job);
            throw new NoSuchJobException(jobId);
        }

        jobStatusChecker.updateStatus(job);
        return job;
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({NullPointerException.class, IllegalArgumentException.class})
    public void onBadRequest(Exception e) {
         log.debug("Exception: {}", e);
    }

    @VisibleForTesting
    static Map<FileFormat, FileStatus> generateFileNames(final CreateJobRequest request) {
        final String baseFileName = generateBaseFileName(request);
        final Map<FileFormat, FileStatus> result = new EnumMap<>(FileFormat.class);
        for (FileFormat format : request.getFileFormats()) {
            result.put(format, new FileStatus(baseFileName + format.getExtension()));
        }
        return result;
    }

    @VisibleForTesting
    static String generateBaseFileName(final CreateJobRequest request) {
        try {
            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(request.getDataSetId().toString().getBytes(StandardCharsets.UTF_8));
            sha256.update(request.getSortedDimensionFilters().toString().getBytes(StandardCharsets.UTF_8));
            return filenameEncoder.encodeToString(sha256.digest());
        } catch (NoSuchAlgorithmException e) {
            log.warn("SHA-256 not available - falling back on random uuid");
            return UUID.randomUUID().toString();
        }
    }
}
