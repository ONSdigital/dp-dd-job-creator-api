package uk.co.onsdigital.job.service;

import com.amazonaws.services.s3.AmazonS3;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.web.util.UriTemplate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.model.FileDto;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class JobDtoStatusCheckerTest {
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
        JobDto jobDto = new JobDto();
        jobDto.setId("x");
        jobDto.setStatus(StatusDto.COMPLETE);
        jobDto.setExpiryTime(now());
        checker.updateStatus(jobDto);
        verifyNoMoreInteractions(mockS3Client);
    }

    @Test
    public void shouldLeaveJobPendingIfFilesNotAvailable() {
        FileDto fileDto = new FileDto("test.csv");
        JobDto jobDto = new JobDto();
        jobDto.setId("x");
        jobDto.setFiles(Collections.singletonList(fileDto));
        jobDto.setStatus(StatusDto.PENDING);
        jobDto.setExpiryTime(now());

        checker.updateStatus(jobDto);

        verify(mockS3Client).doesObjectExist(BUCKET, "test.csv");
        assertThat(fileDto.isComplete()).isFalse();
        assertThat(jobDto.isComplete()).isFalse();
    }

    @Test
    public void shouldMarkFileAsCompleteWhenAvailable() {
        FileDto a = new FileDto("a.csv");
        FileDto b = new FileDto("b.csv");
        JobDto jobDto = new JobDto();
        jobDto.setId("x");
        jobDto.setFiles(Arrays.asList(a, b));
        jobDto.setStatus(StatusDto.PENDING);
        jobDto.setExpiryTime(now());
        when(mockS3Client.doesObjectExist(BUCKET, "b.csv")).thenReturn(true);

        checker.updateStatus(jobDto);

        assertThat(a.isComplete()).isFalse();
        assertThat(b.isComplete()).isTrue();
        assertThat(jobDto.isComplete()).isFalse();

        assertThat(b.getUrl()).isEqualTo(DOWNLOAD_URI.expand("b.csv").toString());
    }

    @Test
    public void shouldMarkJobAsCompleteWhenAllFilesAvailable() {
        FileDto a = new FileDto("a.csv");
        FileDto b = new FileDto("b.csv");
        JobDto jobDto = new JobDto();
        jobDto.setId("x");
        jobDto.setFiles(Arrays.asList(a, b));
        jobDto.setStatus(StatusDto.PENDING);
        jobDto.setExpiryTime(now());
        when(mockS3Client.doesObjectExist(BUCKET, "b.csv")).thenReturn(true);
        when(mockS3Client.doesObjectExist(BUCKET, "a.csv")).thenReturn(true);

        checker.updateStatus(jobDto);

        assertThat(a.isComplete()).isTrue();
        assertThat(b.isComplete()).isTrue();
        assertThat(jobDto.isComplete()).isTrue();
    }

    private static Date now() { return new Date(); }
}