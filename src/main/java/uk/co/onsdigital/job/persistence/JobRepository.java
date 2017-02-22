package uk.co.onsdigital.job.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.discovery.model.Job;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import java.util.Date;


@Component
public class JobRepository {

    @PersistenceContext
    private final EntityManager entityManager;

    @Autowired
    public JobRepository(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public Long countJobsWithStatus(StatusDto statusDto) {
        return entityManager.createNamedQuery(Job.COUNT_JOBS_WITH_STATUS, Long.class).setParameter(Job.STATUS_PARAM, StatusDto.convertToModel(statusDto))
                .getSingleResult();
    }

    public void deleteJobsExpiringBefore(Date before) {
        entityManager.createNamedQuery(Job.DELETE_JOBS_EXPIRING_BEFORE).setParameter(Job.BEFORE_DATE_PARAM, before);
    }

    public void save(JobDto jobDto) {
        entityManager.persist(jobDto.convertToModel());
    }

    public JobDto findOne(String jobId) throws NoResultException {
        Job job = entityManager.createNamedQuery(Job.FIND_ONE_QUERY, Job.class).setParameter(Job.ID_PARAM, jobId).getSingleResult();
        return JobDto.convertFromModel(job);
    }

    public void delete(String jobId) {
        entityManager.createNamedQuery(Job.DELETE_ONE_QUERY).setParameter(Job.ID_PARAM, jobId);
    }
}
