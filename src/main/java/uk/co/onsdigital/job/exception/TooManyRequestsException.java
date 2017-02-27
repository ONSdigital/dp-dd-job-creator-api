package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates that we cannot process a request because there are too many incomplete jobs.
 */
public class TooManyRequestsException extends JobCreatorException {
    public TooManyRequestsException(String message) {
        super(message, HttpStatus.TOO_MANY_REQUESTS);
    }
}
