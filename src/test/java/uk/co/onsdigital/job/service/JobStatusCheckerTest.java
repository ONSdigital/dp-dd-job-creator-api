package uk.co.onsdigital.job.service;

import com.amazonaws.services.s3.AmazonS3;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.web.util.UriTemplate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.model.Status;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class JobStatusCheckerTest {
    private static final String BUCKET = "test-bucket";
    private static final UriTemplate DOWNLOAD_URI = new UriTemplate("http://example.com/download/{filename}");


    @Mock
    private AmazonS3 mockS3Client;

    private JobStatusChecker checker;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        checker = new JobStatusChecker(mockS3Client, BUCKET, DOWNLOAD_URI);
    }

    @Test
    public void shouldNotDoAnythingIfJobIsComplete() {
        Job job = Job.builder().id("x").status(Status.COMPLETE).expiryTime(now()).build();
        checker.updateStatus(job);
        verifyNoMoreInteractions(mockS3Client);
    }

    @Test
    public void shouldLeaveJobPendingIfFilesNotAvailable() {
        FileStatus fileStatus = new FileStatus("test.csv");
        Job job = Job.builder().id("x").file(fileStatus).status(Status.PENDING).expiryTime(now()).build();

        checker.updateStatus(job);

        verify(mockS3Client).doesObjectExist(BUCKET, "test.csv");
        assertThat(fileStatus.isComplete()).isFalse();
        assertThat(job.isComplete()).isFalse();
    }

    @Test
    public void shouldMarkFileAsCompleteWhenAvailable() {
        FileStatus a = new FileStatus("a.csv");
        FileStatus b = new FileStatus("b.csv");
        Job job = Job.builder().id("x").file(a).file(b).status(Status.PENDING).expiryTime(now()).build();
        when(mockS3Client.doesObjectExist(BUCKET, "b.csv")).thenReturn(true);

        checker.updateStatus(job);

        assertThat(a.isComplete()).isFalse();
        assertThat(b.isComplete()).isTrue();
        assertThat(job.isComplete()).isFalse();

        assertThat(b.getUrl()).isEqualTo(DOWNLOAD_URI.expand("b.csv").toString());
    }

    @Test
    public void shouldMarkJobAsCompleteWhenAllFilesAvailable() {
        FileStatus a = new FileStatus("a.csv");
        FileStatus b = new FileStatus("b.csv");
        Job job = Job.builder().id("x").file(a).file(b).status(Status.PENDING).expiryTime(now()).build();
        when(mockS3Client.doesObjectExist(BUCKET, "b.csv")).thenReturn(true);
        when(mockS3Client.doesObjectExist(BUCKET, "a.csv")).thenReturn(true);

        checker.updateStatus(job);

        assertThat(a.isComplete()).isTrue();
        assertThat(b.isComplete()).isTrue();
        assertThat(job.isComplete()).isTrue();
    }

    private static Date now() { return new Date(); }
}