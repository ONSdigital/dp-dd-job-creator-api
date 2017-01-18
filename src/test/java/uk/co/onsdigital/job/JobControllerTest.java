package uk.co.onsdigital.job;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;
import uk.co.onsdigital.job.exception.NoSuchJobException;
import uk.co.onsdigital.job.model.CreateJobRequest;
import uk.co.onsdigital.job.model.DimensionFilter;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.model.Status;
import uk.co.onsdigital.job.repository.DataSetRepository;
import uk.co.onsdigital.job.repository.JobRepository;
import uk.co.onsdigital.job.service.FilterServiceClient;
import uk.co.onsdigital.job.service.JobStatusChecker;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class JobControllerTest {

    @Mock
    private DataSetRepository mockDataSetRepository;

    @Mock
    private JobRepository mockJobRepository;

    @Mock
    private FilterServiceClient mockFilterServiceClient;

    @Mock
    private JobStatusChecker mockJobStatusChecker;

    @Captor
    private ArgumentCaptor<Map<FileFormat, FileStatus>> fileStatus;

    private JobController jobController;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);

        jobController = new JobController(mockDataSetRepository, mockFilterServiceClient, mockJobRepository,
                mockJobStatusChecker);
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

        Map<FileFormat, FileStatus> result = JobController.generateFileNames(request);

        assertThat(result).containsOnlyKeys(FileFormat.CSV);
        assertThat(result.get(FileFormat.CSV).getStatus()).isEqualTo(Status.PENDING);
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
            Job job = (Job)ctx.getArguments()[0];
            job.setStatus(Status.COMPLETE);
            return null;
        }).when(mockJobStatusChecker).updateStatus(any(Job.class));

        jobController.createJob(request);

        verifyZeroInteractions(mockFilterServiceClient);
        verify(mockJobRepository).save(any(Job.class));
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
        assertThat(fileStatus.getValue().get(FileFormat.CSV).getStatus()).isEqualTo(Status.PENDING);
        verify(mockJobRepository).save(any(Job.class));
    }

    @Test
    public void shouldReturnJobStatusFromCreateRequest() throws Exception {
        CreateJobRequest request = request(UUID.randomUUID());
        when(mockDataSetRepository.findS3urlForDataSet(request.getDataSetId())).thenReturn("s3_url");
        when(mockJobRepository.save(any(Job.class))).thenAnswer(ctx -> ctx.getArguments()[0]);

        Job job = jobController.createJob(request);

        assertThat(job).isNotNull();
        assertThat(job.getId()).isNotEmpty();
        assertThat(job.getStatus()).isEqualTo(Status.PENDING);
        assertThat(job.getExpiryTime()).isAfter(new Date());
        assertThat(job.getFiles().get(0).getStatus()).isEqualTo(Status.PENDING);
    }

    @Test(expectedExceptions = NoSuchJobException.class)
    public void shouldFailIfJobDoesntExist() throws Exception {
        jobController.checkJobStatus("no such job");
    }

    @Test
    public void shouldDeleteJobsThatHaveExpired() throws Exception {
        String jobId = "job1";
        Job job = Job.builder().id(jobId).status(Status.PENDING).expiryTime(new Date(0L)).build();
        when(mockJobRepository.getOne(jobId)).thenReturn(job);

        try {
            jobController.checkJobStatus(jobId);
            fail("Expected exception for expired job");
        } catch (NoSuchJobException ex) {
            // Expected
        }

        verify(mockJobRepository).delete(job);
    }

    @Test
    public void shouldCheckStatusForJobsThatArePending() throws Exception {
        String jobId = "job1";
        FileStatus file = new FileStatus("test.csv");
        Job job = Job.builder().id(jobId).status(Status.PENDING).file(file).expiryTime(new Date(Long.MAX_VALUE)).build();
        when(mockJobRepository.getOne(jobId)).thenReturn(job);

        Job result = jobController.checkJobStatus(jobId);

        verify(mockJobStatusChecker).updateStatus(job);
        assertThat(result).isEqualTo(job);
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