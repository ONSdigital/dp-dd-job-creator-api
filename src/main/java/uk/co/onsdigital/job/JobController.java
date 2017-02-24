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
import uk.co.onsdigital.job.exception.NoSuchDataSetException;
import uk.co.onsdigital.job.exception.NoSuchJobException;
import uk.co.onsdigital.job.exception.TooManyRequestsException;
import uk.co.onsdigital.job.model.CreateJobRequest;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatusDto;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.persistence.DataSetRepository;
import uk.co.onsdigital.job.persistence.JobRepository;
import uk.co.onsdigital.job.service.FilterServiceClient;
import uk.co.onsdigital.job.service.JobStatusChecker;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static uk.co.onsdigital.job.model.StatusDto.PENDING;

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
    public JobDto createJob(final @RequestBody CreateJobRequest request) throws JsonProcessingException {
        log.debug("Processing jobDto request: {}", request);
        final String dataSetS3Url;
        dataSetS3Url = dataSetRepository.findS3urlForDataSet(request.getDataSetId());
        final Map<FileFormat, FileStatusDto> files = getInitialFileStatus(request);

        final JobDto jobDto = new JobDto(files.values(), new Date(now().plus(1, HOURS).toEpochMilli()));

        // Check to see if the files already exist
        jobStatusChecker.updateStatus(jobDto);

        if (jobDto.isComplete()) {
            return jobRepository.save(jobDto);
        }
        if (jobRepository.countJobsWithStatus(PENDING) >= pendingJobLimit) {
            throw new TooManyRequestsException("Sorry - the number of requested jobs exceeds the limit");
        }
        filterServiceClient.submitFilterRequest(dataSetS3Url, files, request.getSortedDimensionFilters());
        return jobRepository.save(jobDto);
    }

    @ExceptionHandler(NoSuchDataSetException.class)
    void handleNoSuchDataSetException(NoSuchDataSetException e, HttpServletResponse response) throws IOException {
        log.error(e.getMessage());
        response.sendError(HttpStatus.BAD_REQUEST.value(), e.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    void handleRuntimeException(RuntimeException e, HttpServletResponse response) throws IOException {
        log.error("Unexpected RuntimeException!", e);
        response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
    }

    @GetMapping("/job/{id}")
    @ResponseBody
    @CrossOrigin
    @Transactional
    public JobDto checkJobStatus(final @PathVariable("id") String jobId) {
        log.debug("Checking status for: {}", jobId);
        JobDto jobDto = jobRepository.findOne(jobId);
        if (jobDto == null) {
            throw new NoSuchJobException(jobId);
        }

        if (jobDto.getExpiryTime().before(new Date())) {
            log.debug("Deleting expired jobDto: {}", jobDto);
            jobRepository.delete(jobDto.getId());
            throw new NoSuchJobException(jobId);
        }

        jobStatusChecker.updateStatus(jobDto);
        return jobDto;
    }

    @GetMapping("/healthcheck")
    public boolean healthCheck() {
        return true;
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({NullPointerException.class, IllegalArgumentException.class})
    public void onBadRequest(Exception e) {
         log.debug("Exception: {}", e);
    }

    /**
     * Determines the initial status of any files that need to be generated. The file name will be based on a SHA-256
     * hash of the dataset ID and the (sorted) dimension filters so that all requests for the same filtering of the same
     * dataset will result in the same filename being requested. We check in the database for any existing known status
     * of the files that have been requested to avoid requesting them twice.
     *
     * @param request the request to generate file status for.
     * @return a map from requested file formats to the initial status of the file to be generated.
     */
    @VisibleForTesting
    Map<FileFormat, FileStatusDto> getInitialFileStatus(final CreateJobRequest request) {
        final String baseFileName = generateBaseFileName(request);
        final Map<FileFormat, FileStatusDto> result = new EnumMap<>(FileFormat.class);
        for (FileFormat format : request.getFileFormats()) {
            String fileName = baseFileName + format.getExtension();
            FileStatusDto fileStatusDto = jobRepository.findFileStatus(fileName);
            if (fileStatusDto == null) {
                fileStatusDto = new FileStatusDto(fileName);
            }
            result.put(format, fileStatusDto);
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
