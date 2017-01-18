package uk.co.onsdigital.job.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;

import java.util.UUID;

/**
 * Repository for looking up datasets.
 */
@Repository
public class DataSetRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Looks up the S3 URL for the given dataset.
     *
     * @param dataSetId the id of the dataset to lookup.
     * @return the S3 URL of that dataset.
     * @throws NoSuchDataSetException if the dataset does not exist.
     */
    public String findS3urlForDataSet(UUID dataSetId) {
        try {
            return jdbcTemplate.queryForObject("SELECT ds.s3_url FROM dimensional_data_set ds WHERE ds.dimensional_data_set_id = ?",
                    String.class, dataSetId);
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchDataSetException(dataSetId);
        }
    }

}
