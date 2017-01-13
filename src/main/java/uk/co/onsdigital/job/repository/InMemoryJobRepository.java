package uk.co.onsdigital.job.repository;

import org.springframework.stereotype.Service;
import uk.co.onsdigital.job.model.Job;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Temporary repository for jobs as a quick effort to get things running.
 */
@Service
public class InMemoryJobRepository implements JobRepository {
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    @Override
    public Job save(Job job) {
        final String id = UUID.randomUUID().toString();
        job.setId(id);
        jobs.put(id, job);
        return job;
    }

    @Override
    public Job findOne(String id) {
        Job job = jobs.get(id);
        if (job == null) {
            return null;
        }
        if (job.getExpiryTime().isBefore(Instant.now())) {
            jobs.remove(id);
            return null;
        }
        return job;
    }
}
