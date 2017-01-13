package uk.co.onsdigital.job.repository;

import uk.co.onsdigital.job.model.Job;

/**
 * Repository interface to hold jobs.
 */
public interface JobRepository {
    Job save(Job job);
    Job findOne(String id);
}
