package uk.co.onsdigital.job.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.model.FileDto;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

import javax.persistence.*;
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

    @Transactional
    public void deleteJobsExpiringBefore(Date before) {
        entityManager.createNamedQuery(Job.DELETE_JOBS_EXPIRING_BEFORE).setParameter(Job.BEFORE_DATE_PARAM, before).executeUpdate();
    }

    public JobDto save(JobDto jobDto) {
        return JobDto.convertFromModel(entityManager.merge(jobDto.convertToModel()));
    }

    public JobDto findOne(String jobId) {
        Job job = entityManager.find(Job.class, jobId);
        if (job == null) {
            return null;
        }
        return JobDto.convertFromModel(job);
    }

    public void delete(String jobId) {
        entityManager.createNamedQuery(Job.DELETE_ONE_QUERY).setParameter(Job.ID_PARAM, jobId);
    }

    public FileDto findFileStatus(String filename) {
        File file = entityManager.find(File.class, filename);
        if (file != null) {
            return FileDto.convertFromModel(file);
        }
        return null;
    }
}
