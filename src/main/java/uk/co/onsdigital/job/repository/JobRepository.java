package uk.co.onsdigital.job.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import uk.co.onsdigital.job.model.Job;

/**
 * Repository interface to hold jobs.
 */
@Repository
public interface JobRepository extends JpaRepository<Job, String> {
}
