package uk.co.onsdigital.job.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.job.repository.JobRepository;

import java.util.Date;

/**
 * Runs scheduled tasks.
 */
@Component
public class Scheduler {

    private JobRepository jobRepository;

    @Autowired
    public Scheduler(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    /**
     * Deletes all expired jobs.
     */
    @Scheduled(initialDelay = 1000, fixedRate = 60000)
    void deleteExpiredJobs() {
        jobRepository.deleteJobsExpiringBefore(new Date());
    }
}
