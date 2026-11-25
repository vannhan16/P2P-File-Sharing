package com.nhannv.p2p.ai;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bộ nhớ đệm định danh theo mã băm nội dung <b>SHA-256 Content-Addressed AI Cache</b>
 * (Phase 1 — {@code .p2p-ai-cache.properties}).
 *
 * <h2>Các Cải tiến Hiệu năng &amp; Độ tin cậy (P2-5)</h2>
 * <ul>
 *   <li><b>Debounce Batching (Gộp lô ghi đĩa):</b> Không ghi đĩa đồng bộ mỗi khi put. Dồn các cập nhật
 *       trong 2 giây trước khi ghi một lần duy nhất, tránh thắt nút cổ chai I/O khi quét hàng trăm tệp.</li>
 *   <li><b>Ghi đĩa Nguyên tử (Atomic Write):</b> Ghi vào tệp tạm {@code .tmp} rồi thực hiện
 *       {@link StandardCopyOption#ATOMIC_MOVE} để chống hỏng tệp cache khi ứng dụng bị tắt đột ngột.</li>
 * </ul>
 */
public class AICacheStore implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(AICacheStore.class.getName());
    public static final String DEFAULT_CACHE_FILE = ".p2p-ai-cache.properties";
    private static final String FIELD_DELIM = "||";
    private static final long DEBOUNCE_DELAY_MS = 2_000L;

    private final Path cacheFilePath;
    private final Map<String, AIMetadata> memoryCache = new ConcurrentHashMap<>();
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    private final ScheduledExecutorService flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "p2p-ai-cache-flusher");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> pendingFlush;

    public AICacheStore() {
        this(resolveDefaultCachePath());
    }

    public AICacheStore(Path cacheFilePath) {
        this.cacheFilePath = cacheFilePath;
        loadFromDisk();
    }

    public static Path resolveDefaultCachePath() {
        return Path.of(DEFAULT_CACHE_FILE).toAbsolutePath();
    }

    /**
     * Tra cứu {@link AIMetadata} đã lưu theo mã băm {@code sha256Hash}.
     */
    public Optional<AIMetadata> getBySha256(String sha256Hash) {
        if (sha256Hash == null || sha256Hash.isBlank()) {
            return Optional.empty();
        }
        String key = sha256Hash.trim().toLowerCase(Locale.ROOT);
        return Optional.ofNullable(memoryCache.get(key));
    }

    /**
     * Lưu {@link AIMetadata} mới vào bộ nhớ RAM và xếp lịch ghi đĩa dạng Debounced.
     */
    public void putBySha256(String sha256Hash, AIMetadata metadata) {
        if (sha256Hash == null || sha256Hash.isBlank() || metadata == null || !metadata.hasContent()) {
            return;
        }
        String key = sha256Hash.trim().toLowerCase(Locale.ROOT);
        memoryCache.put(key, metadata);
        dirty.set(true);

        synchronized (this) {
            if (pendingFlush != null && !pendingFlush.isDone()) {
                pendingFlush.cancel(false);
            }
            pendingFlush = flushScheduler.schedule(this::flushToDiskSync, DEBOUNCE_DELAY_MS, TimeUnit.MILLISECONDS);
        }
    }

    public int size() {
        return memoryCache.size();
    }

    private synchronized void loadFromDisk() {
        if (!Files.exists(cacheFilePath)) {
            return;
        }
        Properties props = new Properties();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(cacheFilePath.toFile()))) {
            props.load(bis);
            for (String shaKey : props.stringPropertyNames()) {
                String encoded = props.getProperty(shaKey, "");
                AIMetadata parsed = decodeValue(shaKey, encoded);
                if (parsed != null && parsed.hasContent()) {
                    memoryCache.put(shaKey.toLowerCase(Locale.ROOT), parsed);
                }
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "Không thể nạp AI Cache từ " + cacheFilePath, e);
        }
    }

    /**
     * Ghi toàn bộ cache từ RAM xuống đĩa một cách nguyên tử (Atomic Move) qua tệp tạm.
     */
    public synchronized void flushToDiskSync() {
        if (!dirty.compareAndSet(true, false)) {
            return;
        }

        Properties props = new Properties();
        for (Map.Entry<String, AIMetadata> entry : memoryCache.entrySet()) {
            AIMetadata m = entry.getValue();
            String encoded = m.tags() + FIELD_DELIM
                    + m.summary() + FIELD_DELIM
                    + m.sensitiveWarning() + FIELD_DELIM
                    + m.provider();
            props.setProperty(entry.getKey(), encoded);
        }

        Path parent = cacheFilePath.getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException ignored) {
            }
        }

        Path tempFile = cacheFilePath.resolveSibling(cacheFilePath.getFileName().toString() + ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(tempFile.toFile()))) {
            props.store(bos, "P2P File Sharing - SHA-256 Content-Addressed AI Metadata Cache");
            bos.flush();
            Files.move(tempFile, cacheFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            LOG.log(Level.FINE, "Không thể ghi nguyên tử AI Cache xuống " + cacheFilePath, e);
        }
    }

    private static AIMetadata decodeValue(String shaKey, String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return null;
        }
        String[] parts = encoded.split("\\|\\|", 4);
        String tags = parts.length > 0 ? parts[0] : "";
        String summary = parts.length > 1 ? parts[1] : "";
        String warning = parts.length > 2 ? parts[2] : "";
        String provider = parts.length > 3 ? parts[3] : "Cached AI";
        return new AIMetadata(shaKey, tags, summary, warning, provider);
    }

    @Override
    public void close() {
        flushToDiskSync();
        flushScheduler.shutdownNow();
    }
}
