package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;

/**
 * Base exception that includes details of the status code to return.
 */
public class JobCreatorException extends RuntimeException {

    private final HttpStatus httpStatus;

    public JobCreatorException(String message, HttpStatus httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}
