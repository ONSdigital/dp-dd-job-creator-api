package uk.co.onsdigital.job.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import java.util.UUID;

/**
 * Created by neil on 12/01/2017.
 */
@Repository
public interface DataSetRepository extends JpaRepository<DimensionalDataSet, UUID> {
}
