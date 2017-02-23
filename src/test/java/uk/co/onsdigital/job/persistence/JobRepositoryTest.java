package uk.co.onsdigital.job.persistence;

import com.google.common.collect.ImmutableMap;
import org.eclipse.persistence.config.EntityManagerProperties;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.platform.database.H2Platform;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.model.FileStatusDto;
import uk.co.onsdigital.job.model.JobDto;
import uk.co.onsdigital.job.model.StatusDto;

import javax.persistence.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.time.Instant.now;
import static org.assertj.core.api.Assertions.assertThat;

public class JobRepositoryTest {

    private EntityManagerFactory emf;
    private EntityManager entityManager;
    private JobRepository jobRepository;

    private EntityTransaction transaction;

    @BeforeClass
    public void setupPersistenceContext() {
        this.emf = getInMemoryEntityManagerFactory();
    }

    @BeforeMethod
    public void setup() {
        this.entityManager = emf.createEntityManager();
        this.jobRepository = new JobRepository(entityManager);

        this.transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterMethod
    public void rollback() {
        transaction.rollback();
    }

    @Test
    public void shouldCountJobsCorrectly() {
        int numPending = 42;
        int numComplete = 31;
        for (int i = 0; i < numPending; ++i) {
            entityManager.merge(createJob(Status.PENDING));
        }
        for (int i = 0; i < numComplete; ++i) {
            entityManager.merge(createJob(Status.COMPLETE));
        }

        assertThat(jobRepository.countJobsWithStatus(StatusDto.PENDING)).isEqualTo(numPending);
        assertThat(jobRepository.countJobsWithStatus(StatusDto.COMPLETE)).isEqualTo(numComplete);
    }

    @Test
    public void shouldDeleteExpiredJobs() {
        // Given
        Job expiredJob = createJob(now().minus(2, ChronoUnit.DAYS));
        entityManager.persist(expiredJob);
        assertThat(jobRepository.findOne(expiredJob.getId())).isNotNull();

        // When
        jobRepository.deleteJobsExpiringBefore(new Date());

        // Then
        assertThat(entityManager.find(Job.class, expiredJob.getId())).isNull();
    }

    @Test
    public void shouldNotDeleteSharedFileStatusEntities() {
        // Save two jobs that both refer to the same files with different expiry times
        Job job1 = createJob(now().minus(2, ChronoUnit.DAYS));
        Job job2 = createJob(now().plus(2, ChronoUnit.DAYS));
        job2.setFiles(job1.getFiles());
        int numFiles = job1.getFiles().size();
        List<String> fileNames = job1.getFiles().stream().map(FileStatus::getName).collect(Collectors.toList());
        entityManager.merge(job1);
        entityManager.merge(job2);

        // When
        jobRepository.deleteJobsExpiringBefore(new Date());

        // Then
        // Expired job should have been deleted
        assertThat(entityManager.find(Job.class, job1.getId())).isNull();
        // Non-expired job should still be available with all files still present
        JobDto validJob = jobRepository.findOne(job2.getId());
        assertThat(validJob).isNotNull();
        assertThat(validJob.getFiles()).isNotNull().hasSize(numFiles);
        assertThat(validJob.getFiles()).extracting("name").containsAll(fileNames);
    }

    @Test
    public void shouldSaveJobsCorrectly() {
        // Given
        JobDto jobDto = new JobDto();
        jobDto.setId(UUID.randomUUID().toString());
        jobDto.setExpiryTime(new Date(System.currentTimeMillis() + 60000L));
        jobDto.setFiles(Collections.singletonList(new FileStatusDto("test.csv")));
        jobDto.setStatus(StatusDto.PENDING);

        // When
        jobRepository.save(jobDto);

        // Then
        Job job = entityManager.find(Job.class, jobDto.getId());
        assertThat(job).isNotNull();
        assertThat(job.getId()).isEqualTo(jobDto.getId());
        assertThat(job.getExpiryTime()).isEqualTo(jobDto.getExpiryTime());
        assertThat(job.getStatus()).isEqualTo(Status.PENDING);
        assertThat(job.getFiles()).hasSize(1).extracting("name").containsExactly("test.csv");
    }

    private Job createJob(Status status) {
        Job job = createJob(now().plus(1, ChronoUnit.HOURS));
        job.setStatus(status);
        job.getFiles().forEach(f -> f.setStatus(status));
        return job;
    }

    private Job createJob(Instant expiry) {
        Job job = new Job();
        job.setId(UUID.randomUUID().toString());
        job.setStatus(Status.PENDING);
        job.setExpiryTime(new Date(expiry.toEpochMilli()));

        FileStatus file = new FileStatus("test.csv");
        job.setFiles(Collections.singletonList(file));

        return job;
    }

    private static EntityManagerFactory getInMemoryEntityManagerFactory() {
        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put(EntityManagerProperties.JDBC_URL, "jdbc:h2:mem:test")
                .put(EntityManagerProperties.JDBC_USER, "SA")
                .put(EntityManagerProperties.JDBC_PASSWORD, "")
                .put(EntityManagerProperties.JDBC_DRIVER, "org.h2.Driver")
                .put(PersistenceUnitProperties.DDL_GENERATION, PersistenceUnitProperties.DROP_AND_CREATE)
                .put(PersistenceUnitProperties.DDL_GENERATION_MODE, PersistenceUnitProperties.DDL_DATABASE_GENERATION)
                .put(PersistenceUnitProperties.TARGET_DATABASE, H2Platform.class.getName())
                .build();

        return Persistence.createEntityManagerFactory("data_discovery", props);
    }
}