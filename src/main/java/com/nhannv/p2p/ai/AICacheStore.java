package com.nhannv.p2p.ai;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bộ nhớ đệm định danh theo mã băm nội dung <b>SHA-256 Content-Addressed AI Cache</b>
 * (Phase 1 — {@code .p2p-ai-cache.properties}).
 *
 * <h2>Nguyên lý Tiết kiệm Token &amp; Phản hồi Tức thời</h2>
 * <ul>
 *   <li>Khóa tra cứu là mã băm {@code sha256Hash} (64 ký tự hex) của tệp.</li>
 *   <li>Khi {@code WatchService} hoặc người dùng quét thư mục {@code Shared}, nếu mã {@code sha256Hash}
 *       đã có trong Cache, hệ thống trả về ngay {@link AIMetadata} trong {@code < 0.1 ms} mà không bao giờ
 *       phải gọi lại Gemini/Ollama cho cùng một nội dung tệp.</li>
 * </ul>
 */
public class AICacheStore {

    private static final Logger LOG = Logger.getLogger(AICacheStore.class.getName());
    public static final String DEFAULT_CACHE_FILE = ".p2p-ai-cache.properties";
    private static final String FIELD_DELIM = "||";

    private final Path cacheFilePath;
    private final Map<String, AIMetadata> memoryCache = new ConcurrentHashMap<>();

    public AICacheStore() {
        this(Path.of(DEFAULT_CACHE_FILE).toAbsolutePath());
    }

    public AICacheStore(Path cacheFilePath) {
        this.cacheFilePath = cacheFilePath;
        loadFromDisk();
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
     * Lưu {@link AIMetadata} mới vào bộ nhớ RAM và đồng bộ xuống tệp {@code .p2p-ai-cache.properties}.
     */
    public void putBySha256(String sha256Hash, AIMetadata metadata) {
        if (sha256Hash == null || sha256Hash.isBlank() || metadata == null || !metadata.hasContent()) {
            return;
        }
        String key = sha256Hash.trim().toLowerCase(Locale.ROOT);
        memoryCache.put(key, metadata);
        saveToDiskSync();
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

    private synchronized void saveToDiskSync() {
        Properties props = new Properties();
        for (Map.Entry<String, AIMetadata> entry : memoryCache.entrySet()) {
            AIMetadata m = entry.getValue();
            String encoded = m.tags() + FIELD_DELIM
                    + m.summary() + FIELD_DELIM
                    + m.sensitiveWarning() + FIELD_DELIM
                    + m.provider();
            props.setProperty(entry.getKey(), encoded);
        }
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(cacheFilePath.toFile()))) {
            props.store(bos, "P2P File Sharing - SHA-256 Content-Addressed AI Metadata Cache");
        } catch (IOException e) {
            LOG.log(Level.FINE, "Không thể ghi AI Cache xuống " + cacheFilePath, e);
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
}
