package uk.co.onsdigital.job.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


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
        Job job = jobDto.convertToModel();
        List<FileStatus> files = job.getFiles().stream().map(entityManager::merge).collect(Collectors.toList());
        job.setFiles(files);
        entityManager.persist(job);
    }

    public JobDto findOne(String jobId) throws NoResultException {
        Job job = entityManager.createNamedQuery(Job.FIND_ONE_QUERY, Job.class).setParameter(Job.ID_PARAM, jobId).getSingleResult();
        return JobDto.convertFromModel(job);
    }

    public void delete(String jobId) {
        entityManager.createNamedQuery(Job.DELETE_ONE_QUERY).setParameter(Job.ID_PARAM, jobId);
    }
}
