package uk.co.onsdigital.job.repository;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.model.Status;

import javax.transaction.Transactional;
import java.util.Date;

/**
 * Repository interface to hold jobs.
 */
@Repository
public interface JobRepository extends JpaRepository<Job, String> {

    /**
     * @param status the status to count for.
     * @return the count of jobs with the given status.
     */
    @Query("select count(j) from Job j where j.status=:status")
    Long countJobsWithStatus(@Param("status") Status status);

    /**
     * Deletes all jobs with an expiryTime before the given time.
     * @param before The expiry time.
     */
    @Modifying
    @Transactional
    @Query("delete from Job j where j.expiryTime < :before")
    void deleteJobsExpiringBefore(@Param("before") Date before);
}
