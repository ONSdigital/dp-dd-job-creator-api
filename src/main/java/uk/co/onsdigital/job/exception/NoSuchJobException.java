package uk.co.onsdigital.job.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Indicates that a job does not exist in the system.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class NoSuchJobException extends RuntimeException {
    public NoSuchJobException(String message) {
        super(message);
    }
}
