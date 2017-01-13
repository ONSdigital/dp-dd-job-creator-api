package uk.co.onsdigital.job.model;

/**
 * Supported file generation formats.
 */
public enum FileFormat {
    CSV;

    public String getExtension() {
        return "." + name().toLowerCase();
    }
}
