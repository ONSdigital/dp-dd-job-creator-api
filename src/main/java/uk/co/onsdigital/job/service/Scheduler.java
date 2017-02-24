package uk.co.onsdigital.job.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.job.persistence.JobRepository;

import java.util.Date;

/**
 * Runs scheduled tasks.
 */
@Component
public class Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    private JobRepository jobRepository;

    @Autowired
    public Scheduler(JobRepository jobRepository) {
        log.info("Starting job cleanup task");
        this.jobRepository = jobRepository;
    }

    /**
     * Deletes all expired jobs.
     */
    @Scheduled(initialDelay = 1000, fixedRate = 60000)
    void deleteExpiredJobs() {
        log.debug("Deleting expired jobs");
        jobRepository.deleteJobsExpiringBefore(new Date());
    }
}
