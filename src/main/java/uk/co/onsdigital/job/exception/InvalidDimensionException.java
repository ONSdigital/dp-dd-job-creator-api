package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates that the requested dataset does not contain the requested dimension/value combination.
 */
public class InvalidDimensionException extends JobCreatorException {
    public InvalidDimensionException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }
}
