package uk.co.onsdigital.job.service;

import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.repository.JobRepository;

import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.verify;

public class SchedulerTest {

    @Mock
    private JobRepository jobRepositoryMock;

    private ArgumentCaptor<Long> captor;

    private Scheduler testObj;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        testObj = new Scheduler(jobRepositoryMock);
    }

    @Test
    public void testDeleteExpiredJobs() throws Exception {
        Date start = new Date();
        ArgumentCaptor<Date> captor = ArgumentCaptor.forClass(Date.class);

        testObj.deleteExpiredJobs();
        verify(jobRepositoryMock).deleteJobsExpiringBefore(captor.capture());
        assertThat(captor.getValue(), is(greaterThanOrEqualTo(start)));
        assertThat(captor.getValue(), is(lessThanOrEqualTo(new Date())));
    }

}