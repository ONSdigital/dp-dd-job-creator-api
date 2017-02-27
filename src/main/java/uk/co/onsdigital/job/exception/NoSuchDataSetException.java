package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.UUID;

/**
 * Indicates that the requested dataset does not exist in the database.
 */
public class NoSuchDataSetException extends JobCreatorException {
    public NoSuchDataSetException(UUID datasetId) {
        super("Dataset not found: " + datasetId, HttpStatus.BAD_REQUEST);
    }
}
