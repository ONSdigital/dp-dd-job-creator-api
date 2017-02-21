package uk.co.onsdigital.job.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;

import javax.persistence.EntityManager;
import java.util.UUID;

/**
 * Repository for looking up datasets.
 */
@Component
public class DataSetRepository {

    private final EntityManager entityManager;

    @Autowired
    public DataSetRepository(EntityManager entityManager) {
        this.entityManager = entityManager;
    }


    /**
     * Looks up the S3 URL for the given dataset.
     *
     * @param dataSetId the id of the dataset to lookup.
     * @return the S3 URL of that dataset.
     * @throws NoSuchDataSetException if the dataset does not exist.
     */
    public String findS3urlForDataSet(UUID dataSetId) {
        try {
            return entityManager.createNamedQuery(DimensionalDataSet.FIND_BY_ID, DimensionalDataSet.class)
                    .setParameter(DimensionalDataSet.ID_PARAM, dataSetId).getSingleResult().getS3URL();
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchDataSetException(dataSetId);
        }
    }

}
