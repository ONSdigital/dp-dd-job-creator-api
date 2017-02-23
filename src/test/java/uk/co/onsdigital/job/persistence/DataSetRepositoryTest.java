package uk.co.onsdigital.job.persistence;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class DataSetRepositoryTest extends AbstractInMemoryDatabaseTests {

    private DataSetRepository dataSetRepository;

    @BeforeMethod
    public void setupRepository() {
        this.dataSetRepository = new DataSetRepository(entityManager);
    }

    @Test
    public void shouldReturnS3UrlForExistingDatasets() {
        // Given
        UUID id = UUID.randomUUID();
        String s3Url = "some s3 url";
        DimensionalDataSet dataSet = new DimensionalDataSet();
        dataSet.setId(id);
        dataSet.setS3URL(s3Url);
        entityManager.persist(dataSet);

        // When
        String result = dataSetRepository.findS3urlForDataSet(id);

        // Then
        assertThat(result).isEqualTo(s3Url);
    }

    @Test(expectedExceptions = NoSuchDataSetException.class)
    public void shouldThrowNoSuchDataSetExceptionIfNotFound() {
        dataSetRepository.findS3urlForDataSet(UUID.randomUUID());
    }

}