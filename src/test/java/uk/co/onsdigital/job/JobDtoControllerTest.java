package uk.co.onsdigital.job;

import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.exception.*;
import uk.co.onsdigital.job.model.*;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.persistence.DataSetRepository;
import uk.co.onsdigital.job.persistence.JobRepository;
import uk.co.onsdigital.job.service.FilterServiceClient;
import uk.co.onsdigital.job.service.JobStatusChecker;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.onsdigital.job.model.StatusDto.PENDING;

public class JobDtoControllerTest {

    @Mock
    private DataSetRepository mockDataSetRepository;

    @Mock
    private JobRepository mockJobRepository;

    @Mock
    private FilterServiceClient mockFilterServiceClient;

    @Mock
    private JobStatusChecker mockJobStatusChecker;

    @Captor
    private ArgumentCaptor<Map<FileFormat, FileStatusDto>> fileStatus;

    private long pendingJobLimit = 99;

    private JobController jobController;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);

        jobController = new JobController(mockDataSetRepository, mockFilterServiceClient, mockJobRepository,
                mockJobStatusChecker, pendingJobLimit);

        when(mockJobRepository.countJobsWithStatus(PENDING)).thenReturn(0L);
    }

    @Test
    public void shouldCreateSameFilenameForIdenticalJobs() throws Exception {
        UUID dataSetId = UUID.randomUUID();
        CreateJobRequest request1 = request(dataSetId);
        CreateJobRequest request2 = request(dataSetId);

        String filename1 = JobController.generateBaseFileName(request1);
        String filename2 = JobController.generateBaseFileName(request2);

        assertThat(filename1).isEqualTo(filename2);
    }

    @Test
    public void shouldCreateDifferentFilenamesForDifferentDataSets() throws Exception {
        CreateJobRequest request1 = request(UUID.randomUUID());
        CreateJobRequest request2 = request(UUID.randomUUID());

        String filename1 = JobController.generateBaseFileName(request1);
        String filename2 = JobController.generateBaseFileName(request2);

        assertThat(filename1).isNotEqualTo(filename2);
    }

    @Test
    public void shouldCreateDifferentFilenamesForDifferentFilters() throws Exception {
        UUID dataSetId = UUID.randomUUID();

        CreateJobRequest request1 = request(dataSetId);
        CreateJobRequest request2 = request(dataSetId);
        request1.setDimensions(asList(new DimensionFilter("third", asList("e"))));

        String filename1 = JobController.generateBaseFileName(request1);
        String filename2 = JobController.generateBaseFileName(request2);

        assertThat(filename1).isNotEqualTo(filename2);
    }

    @Test
    public void shouldCreatePendingInitialFileStatus() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());

        Map<FileFormat, FileStatusDto> result = JobController.generateFileNames(request);

        assertThat(result).containsOnlyKeys(FileFormat.CSV);
        assertThat(result.get(FileFormat.CSV).getStatus()).isEqualTo(PENDING);
        assertThat(result.get(FileFormat.CSV).isComplete()).isFalse();
        assertThat(result.get(FileFormat.CSV).getUrl()).isNull();
        assertThat(result.get(FileFormat.CSV).getName()).isNotNull().endsWith(".csv");
    }

    @Test(expectedExceptions = NoSuchDataSetException.class)
    public void shouldRejectMissingDataSets() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenThrow(new NoSuchDataSetException(null));

        jobController.createJob(request);
    }

    @Test
    public void shouldNotSubmitRequestIfFilesAlreadyExist() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn("s3_url");
        doAnswer(ctx -> {
            JobDto jobDto = (JobDto)ctx.getArguments()[0];
            jobDto.setStatus(StatusDto.COMPLETE);
            return null;
        }).when(mockJobStatusChecker).updateStatus(any(JobDto.class));

        jobController.createJob(request);

        verifyZeroInteractions(mockFilterServiceClient);
        verify(mockJobRepository).save(any(JobDto.class));
    }

    @Test(expectedExceptions = TooManyRequestsException.class)
    public void shouldThrowExceptionWhenPendingLimitExceeded() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn("s3_url");
        when(mockJobRepository.countJobsWithStatus(PENDING)).thenReturn(pendingJobLimit);
        doAnswer(ctx -> {
            JobDto jobDto = (JobDto)ctx.getArguments()[0];
            jobDto.setStatus(PENDING);
            return null;
        }).when(mockJobStatusChecker).updateStatus(any(JobDto.class));

        try {
            jobController.createJob(request);
        } finally {
            verifyZeroInteractions(mockFilterServiceClient);
            verify(mockJobRepository, never()).save(any(JobDto.class));
        }

    }

    @Test
    public void shouldReturnCompletedJobWhenPendingLimitExceeded() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn("s3_url");
        when(mockJobRepository.countJobsWithStatus(PENDING)).thenReturn(pendingJobLimit);
        doAnswer(ctx -> {
            JobDto jobDto = (JobDto)ctx.getArguments()[0];
            jobDto.setStatus(StatusDto.COMPLETE);
            return null;
        }).when(mockJobStatusChecker).updateStatus(any(JobDto.class));

        jobController.createJob(request);

        verifyZeroInteractions(mockFilterServiceClient);
        verify(mockJobRepository).save(any(JobDto.class));
    }

    @Test
    public void shouldSubmitRequestToFilterIfFilesDoNotExist() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        String s3Url = "s3://test/test.csv";
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn(s3Url);

        jobController.createJob(request);

        verify(mockFilterServiceClient).submitFilterRequest(eq(s3Url), fileStatus.capture(),
                eq(request.getSortedDimensionFilters()));
        assertThat(fileStatus.getValue()).containsOnlyKeys(FileFormat.CSV);
        assertThat(fileStatus.getValue().get(FileFormat.CSV).getStatus()).isEqualTo(PENDING);
        verify(mockJobRepository).save(any(JobDto.class));
    }

    @Test
    public void shouldReturnJobStatusFromCreateRequest() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn("s3_url");
        Mockito.doNothing().when(mockJobRepository).save(any(JobDto.class));

        JobDto jobDto = jobController.createJob(request);

        assertThat(jobDto).isNotNull();
        assertThat(jobDto.getId()).isNotEmpty();
        assertThat(jobDto.getStatus()).isEqualTo(PENDING);
        assertThat(jobDto.getExpiryTime()).isAfter(new Date());
        assertThat(jobDto.getFiles().get(0).getStatus()).isEqualTo(PENDING);
    }

    @Test(expectedExceptions = NoSuchJobException.class)
    public void shouldFailIfJobDoesntExist() throws Exception {
        jobController.checkJobStatus("no such job");
    }

    @Test
    public void shouldDeleteJobsThatHaveExpired() throws Exception {
        String jobId = "job1";
        JobDto jobDto = new JobDto();
        jobDto.setId(jobId);
        jobDto.setStatus(PENDING);
        jobDto.setExpiryTime(new Date(0L));
        when(mockJobRepository.findOne(jobId)).thenReturn(jobDto);

        try {
            jobController.checkJobStatus(jobId);
            fail("Expected exception for expired jobDto");
        } catch (NoSuchJobException ex) {
            // Expected
        }

        verify(mockJobRepository).delete(jobDto.getId());
    }

    @Test
    public void shouldCheckStatusForJobsThatArePending() throws Exception {
        String jobId = "job1";
        List<FileStatusDto> files = new LinkedList<FileStatusDto>();
        FileStatusDto file = new FileStatusDto("test.csv");
        files.add(file);
        JobDto jobDto = new JobDto();
        jobDto.setId(jobId);
        jobDto.setStatus(PENDING);
        jobDto.setExpiryTime(new Date(Long.MAX_VALUE));
        jobDto.setFiles(files);
        when(mockJobRepository.findOne(jobId)).thenReturn(jobDto);

        JobDto result = jobController.checkJobStatus(jobId);

        verify(mockJobStatusChecker).updateStatus(jobDto);
        assertThat(result).isEqualTo(jobDto);
    }

    private static CreateJobRequest request(UUID dataSetId) {
        CreateJobRequest request = new CreateJobRequest();
        request.setDataSetId(dataSetId);
        List<DimensionFilter> dimensions = asList(new DimensionFilter("first", asList("a", "b")),
                new DimensionFilter("second", asList("c", "d")));
        Set<FileFormat> formats = singleton(FileFormat.CSV);
        request.setDimensions(dimensions);
        request.setFileFormats(formats);
        return request;
    }
}