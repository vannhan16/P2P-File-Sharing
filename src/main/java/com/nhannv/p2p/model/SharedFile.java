package com.nhannv.p2p.model;

import java.io.File;
import java.util.Locale;
import java.util.Objects;

/**
 * Immutable description of a file that the local peer shares on the LAN.
 *
 * @param name      file name shown to other peers
 * @param sizeBytes file size in bytes
 * @param path      absolute local path of the file
 */
public record SharedFile(String name, long sizeBytes, String path) {

    public SharedFile {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(path, "path");
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be >= 0");
        }
    }

    /** Creates a {@link SharedFile} from a local {@link File}. */
    public static SharedFile from(File file) {
        Objects.requireNonNull(file, "file");
        return new SharedFile(file.getName(), file.length(), file.getAbsolutePath());
    }

    /** Human-readable size, e.g. "1.25 MB". */
    public String formattedSize() {
        if (sizeBytes < 1024) {
            return sizeBytes + " B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double value = sizeBytes;
        int unitIndex = -1;
        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex++;
        }
        return String.format(Locale.US, "%.2f %s", value, units[unitIndex]);
    }
}
