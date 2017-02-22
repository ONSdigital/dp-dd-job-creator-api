package uk.co.onsdigital.job.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import uk.co.onsdigital.discovery.model.Status;

import java.util.Locale;

/**
 * Represents the status of a job or file.
 */
public enum StatusDto {
    COMPLETE, PENDING;

    @JsonCreator
    public static StatusDto fromString(String value) {
        return valueOf(value.toUpperCase(Locale.UK));
    }

    @Override @JsonValue
    public String toString() {
        return name().substring(0, 1) + name().substring(1).toLowerCase(Locale.UK);
    }

    public static Status convertToModel(StatusDto statusDto) {
        switch (statusDto) {
            case COMPLETE:
                return Status.COMPLETE;
            default:
                return Status.PENDING;
        }
    }
}
