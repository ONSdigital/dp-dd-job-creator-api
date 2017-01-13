package uk.co.onsdigital.job.repository;

import uk.co.onsdigital.job.model.Job;

/**
 * Created by neil on 12/01/2017.
 */
public interface JobRepository {
    Job save(Job job);
    Job findOne(String id);
}
