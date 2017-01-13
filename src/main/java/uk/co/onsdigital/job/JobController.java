package uk.co.onsdigital.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.job.exception.NoSuchJobException;
import uk.co.onsdigital.job.model.CreateJobRequest;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.model.Status;
import uk.co.onsdigital.job.repository.DataSetRepository;
import uk.co.onsdigital.job.repository.JobRepository;
import uk.co.onsdigital.job.service.FilterServiceClient;
import uk.co.onsdigital.job.service.JobStatusChecker;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * REST API implementation.
 */
@RestController
public class JobController {
    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final DataSetRepository dataSetRepository;
    private final FilterServiceClient filterServiceClient;
    private final JobRepository jobRepository;
    private final JobStatusChecker jobStatusChecker;

    @Autowired
    JobController(DataSetRepository dataSetRepository, FilterServiceClient filterServiceClient,
                  JobRepository jobRepository, JobStatusChecker jobStatusChecker) {
        this.dataSetRepository = dataSetRepository;
        this.filterServiceClient = filterServiceClient;
        this.jobStatusChecker = jobStatusChecker;
        this.jobRepository = jobRepository;
    }


    @PostMapping(value = "/job", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    @CrossOrigin
    @Transactional
    public Job createJob(final @RequestBody CreateJobRequest request) throws JsonProcessingException {
        log.debug("Processing job request: {}", request);
        final DimensionalDataSet dataSet = dataSetRepository.findOne(request.getDataSetId());
        if (dataSet == null) {
            throw new IllegalArgumentException("No such dataset");
        }

        final Map<FileFormat, FileStatus> statusMap = filterServiceClient.submitFilterRequest(dataSet,
                request.getDimensions(), request.getFileFormats());

        final Job job = Job.builder().status(Status.PENDING)
                .files(new ArrayList<>(statusMap.values()))
                .expiryTime(Instant.now().plus(1, ChronoUnit.HOURS))
                .build();

        return jobRepository.save(job);
    }

    @GetMapping("/job/{id}")
    @ResponseBody
    @CrossOrigin
    @Transactional
    public Job checkJobStatus(final @PathVariable("id") String jobId) {
        log.debug("Checking status for: {}", jobId);
        final Job job = jobRepository.findOne(jobId);
        if (job == null) {
            throw new NoSuchJobException(jobId);
        }
        jobStatusChecker.updateStatus(job);
        return job;
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler({NullPointerException.class, IllegalArgumentException.class})
    public void onBadRequest(Exception e) {
        log.error("Exception: {}", e);
    }

    private static <T> List<T> concatLists(List<T> x, List<T> y) {
        return Stream.concat(x.stream(), y.stream()).collect(toList());
    }

}
