package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;

/**
 * Indicates that a service is currently unavailable.
 */
public class ServiceUnavailableException extends JobCreatorException {
    public ServiceUnavailableException(String message) {
        super(message, HttpStatus.SERVICE_UNAVAILABLE);
    }
}
