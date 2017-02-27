package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates that a job does not exist in the system.
 */
public class NoSuchJobException extends JobCreatorException {
    public NoSuchJobException(String message) {
        super(message, HttpStatus.NOT_FOUND);
    }
}
