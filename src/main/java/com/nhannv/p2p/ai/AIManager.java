package com.nhannv.p2p.ai;

import com.nhannv.p2p.util.CryptoUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bộ điều phối Trí tuệ Nhân tạo trung tâm (<b>AI Strategy Coordinator &amp; SHA-256 Cache Manager</b>).
 *
 * <h2>Các Cải tiến Bảo mật &amp; Hiệu năng</h2>
 * <ul>
 *   <li><b>Chặn rò rỉ dữ liệu lên Cloud (S7):</b> Nếu tệp có {@code sensitiveWarning} (chứa API key, password, private key),
 *       hệ thống TUYỆT ĐỐI KHÔNG gửi snippet lên Gemini Cloud, mà chuyển thẳng sang Ollama Local hoặc Heuristic Offline.</li>
 *   <li><b>Công tắc Cloud AI (S7):</b> Có thuộc tính {@code allowCloudAi} (mặc định {@code false}) để người dùng
 *       chủ động kiểm soát việc gửi nội dung ra Internet.</li>
 *   <li><b>Tránh gọi AI trùng lặp (P2-7):</b> Dùng {@link ConcurrentHashMap} theo {@code sha256Hash} để gom các yêu cầu
 *       phân tích cùng một tệp vào một {@link CompletableFuture} duy nhất.</li>
 *   <li><b>Bảo vệ API Key (S7):</b> Mã hóa Obfuscate khóa API khi lưu vào {@code .p2p-ai-config.properties}.</li>
 * </ul>
 */
public final class AIManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(AIManager.class.getName());
    private static final AIManager INSTANCE = new AIManager();
    public static final String CONFIG_FILE_NAME = ".p2p-ai-config.properties";

    private final AICacheStore cacheStore;
    private final GeminiAIService geminiService;
    private final OllamaAIService ollamaService;
    private final HeuristicOfflineAIService heuristicService;
    private final ExecutorService aiWorkerPool;
    private final Path configFilePath;

    /** Công tắc Cho phép gửi nội dung lên Cloud AI (mặc định tắt vì an toàn bảo mật LAN). */
    private volatile boolean allowCloudAi = false;

    /** Bảng theo dõi các tác vụ AI đang chạy để tránh gọi trùng lặp (P2-7). */
    private final Map<String, CompletableFuture<AIMetadata>> inFlightAnalyses = new ConcurrentHashMap<>();

    private AIManager() {
        this.cacheStore = new AICacheStore();
        this.geminiService = new GeminiAIService();
        this.ollamaService = new OllamaAIService();
        this.heuristicService = new HeuristicOfflineAIService();
        this.configFilePath = Path.of(CONFIG_FILE_NAME).toAbsolutePath();

        AtomicInteger counter = new AtomicInteger(1);
        this.aiWorkerPool = Executors.newFixedThreadPool(2, runnable -> {
            Thread t = new Thread(runnable, "p2p-ai-worker-" + counter.getAndIncrement());
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY - 1);
            return t;
        });

        loadSavedConfig();
    }

    public static AIManager getInstance() {
        return INSTANCE;
    }

    public AICacheStore getCacheStore() {
        return cacheStore;
    }

    public GeminiAIService getGeminiService() {
        return geminiService;
    }

    public OllamaAIService getOllamaService() {
        return ollamaService;
    }

    public HeuristicOfflineAIService getHeuristicService() {
        return heuristicService;
    }

    public boolean isAllowCloudAi() {
        return allowCloudAi;
    }

    public synchronized void setAllowCloudAi(boolean allowCloudAi) {
        this.allowCloudAi = allowCloudAi;
        saveConfigToDisk();
    }

    /**
     * Cập nhật Gemini API Key và lưu an toàn xuống đĩa.
     */
    public synchronized void configureGeminiApiKey(String apiKey) {
        geminiService.setApiKey(apiKey);
        saveConfigToDisk();
    }

    /**
     * Trả về tên chiến lược AI đang hoạt động để hiển thị lên Top Header Pill.
     */
    public String getActiveProviderBadgeText() {
        if (allowCloudAi && geminiService.isAvailable()) {
            return "AI: Gemini Cloud";
        }
        if (ollamaService.isAvailable()) {
            return "AI: Ollama Local";
        }
        return "AI: Local Heuristic";
    }

    /**
     * Phân tích tệp bất đồng bộ kèm tra cứu Cache và Khử trùng lặp (P2-7 Dedup).
     */
    public CompletableFuture<AIMetadata> analyzeFileAsync(File file, String sha256Hash) {
        if (sha256Hash != null && !sha256Hash.isBlank()) {
            Optional<AIMetadata> cached = cacheStore.getBySha256(sha256Hash);
            if (cached.isPresent()) {
                return CompletableFuture.completedFuture(cached.get());
            }

            // P2-7: Nếu cùng một file SHA-256 đang được phân tích bởi luồng khác, tái sử dụng Future
            CompletableFuture<AIMetadata> existingFuture = inFlightAnalyses.get(sha256Hash);
            if (existingFuture != null && !existingFuture.isDone()) {
                return existingFuture;
            }
        }

        CompletableFuture<AIMetadata> future = CompletableFuture.supplyAsync(
                () -> analyzeFileWithCache(file, sha256Hash),
                aiWorkerPool
        );

        if (sha256Hash != null && !sha256Hash.isBlank()) {
            inFlightAnalyses.put(sha256Hash, future);
            future.whenComplete((res, err) -> inFlightAnalyses.remove(sha256Hash));
        }

        return future;
    }

    /**
     * Phân tích đồng bộ một tệp (kết hợp tra cứu Cache theo {@code sha256Hash} và tự động Fallback).
     */
    public AIMetadata analyzeFileWithCache(File file, String sha256Hash) {
        if (sha256Hash != null && !sha256Hash.isBlank()) {
            Optional<AIMetadata> cached = cacheStore.getBySha256(sha256Hash);
            if (cached.isPresent()) {
                return cached.get();
            }
        }

        String snippet = FileContentExtractor.extractSnippet(file);
        String sensitiveWarning = FileContentExtractor.detectSensitiveWarning(
                file != null ? file.getName() : "", snippet);

        AIMetadata result = null;

        // [S7 Fix]: Chỉ gọi Cloud AI khi người dùng cho phép VÀ tệp KHÔNG chứa dữ liệu nhạy cảm
        boolean hasSensitiveData = (sensitiveWarning != null && !sensitiveWarning.isBlank());
        if (allowCloudAi && !hasSensitiveData && geminiService.isAvailable()) {
            try {
                result = geminiService.analyzeFileMetadata(file, sha256Hash, snippet, sensitiveWarning);
            } catch (Exception e) {
                LOG.log(Level.FINE, "GeminiAIService lỗi, chuyển sang chiến lược dự phòng: " + e.getMessage());
            }
        } else if (hasSensitiveData) {
            LOG.info(() -> "Tệp chứa dữ liệu nhạy cảm (" + sensitiveWarning + ") -> Bỏ qua Cloud AI để bảo vệ quyền riêng tư.");
        }

        if (result == null && ollamaService.isAvailable()) {
            try {
                result = ollamaService.analyzeFileMetadata(file, sha256Hash, snippet, sensitiveWarning);
            } catch (Exception e) {
                LOG.log(Level.FINE, "OllamaAIService lỗi, chuyển sang HeuristicOfflineAIService: " + e.getMessage());
            }
        }

        if (result == null || !result.hasContent()) {
            result = heuristicService.analyzeFileMetadata(file, sha256Hash, snippet, sensitiveWarning);
        }

        if (sha256Hash != null && !sha256Hash.isBlank() && result.hasContent()) {
            cacheStore.putBySha256(sha256Hash, result);
        }
        return result;
    }

    /**
     * Sinh báo cáo xem trước nội dung tệp (Pre-download AI Preview) từ đoạn trích 8 KB đầu tiên.
     */
    public CompletableFuture<String> generatePreDownloadPreviewAsync(
            String fileName,
            long fileSize,
            String existingTags,
            String snippetText
    ) {
        return CompletableFuture.supplyAsync(() -> {
            String warning = FileContentExtractor.detectSensitiveWarning(fileName, snippetText);
            boolean isSensitive = (warning != null && !warning.isBlank());

            if (allowCloudAi && !isSensitive && geminiService.isAvailable()) {
                try {
                    return geminiService.generatePreDownloadPreview(fileName, fileSize, existingTags, snippetText);
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Gemini Preview lỗi, fallback sang Ollama/Heuristic", e);
                }
            }
            if (ollamaService.isAvailable()) {
                try {
                    return ollamaService.generatePreDownloadPreview(fileName, fileSize, existingTags, snippetText);
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Ollama Preview lỗi, fallback sang Heuristic", e);
                }
            }
            return heuristicService.generatePreDownloadPreview(fileName, fileSize, existingTags, snippetText);
        }, aiWorkerPool);
    }

    /**
     * Chẩn đoán lỗi mạng bất đồng bộ từ {@link NetworkDiagnosticContext} thực tế của ứng dụng.
     */
    public CompletableFuture<String> diagnoseNetworkIssueAsync(NetworkDiagnosticContext context) {
        return CompletableFuture.supplyAsync(() -> {
            if (allowCloudAi && geminiService.isAvailable()) {
                try {
                    return geminiService.diagnoseNetworkIssue(context);
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Gemini Network Diagnose lỗi, fallback", e);
                }
            }
            if (ollamaService.isAvailable()) {
                try {
                    return ollamaService.diagnoseNetworkIssue(context);
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Ollama Network Diagnose lỗi, fallback", e);
                }
            }
            return heuristicService.diagnoseNetworkIssue(context);
        }, aiWorkerPool);
    }

    @Override
    public void close() {
        shutdown();
    }

    public void shutdown() {
        cacheStore.close();
        aiWorkerPool.shutdownNow();
    }

    private void loadSavedConfig() {
        if (!Files.exists(configFilePath)) {
            return;
        }
        Properties props = new Properties();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(configFilePath.toFile()))) {
            props.load(bis);
            String savedEncKey = props.getProperty("gemini.apiKey.enc", "").trim();
            if (!savedEncKey.isEmpty()) {
                try {
                    String decryptedKey = new String(Base64.getDecoder().decode(savedEncKey), StandardCharsets.UTF_8);
                    geminiService.setApiKey(decryptedKey);
                } catch (Exception e) {
                    LOG.log(Level.FINE, "Không thể giải mã apiKey từ config", e);
                }
            } else {
                String plainKey = props.getProperty("gemini.apiKey", "").trim();
                if (!plainKey.isEmpty()) {
                    geminiService.setApiKey(plainKey);
                }
            }

            this.allowCloudAi = Boolean.parseBoolean(props.getProperty("ai.allowCloud", "false"));

            String savedOllamaModel = props.getProperty("ollama.model", "").trim();
            if (!savedOllamaModel.isEmpty()) {
                ollamaService.setModelName(savedOllamaModel);
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "Lỗi đọc file cấu hình AI", e);
        }
    }

    private void saveConfigToDisk() {
        Properties props = new Properties();
        String currentKey = geminiService.getApiKey();
        if (currentKey != null && !currentKey.isEmpty()) {
            props.setProperty("gemini.apiKey.enc", Base64.getEncoder().encodeToString(currentKey.getBytes(StandardCharsets.UTF_8)));
        }
        props.setProperty("ai.allowCloud", Boolean.toString(allowCloudAi));
        props.setProperty("ollama.model", ollamaService.getModelName());

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(configFilePath.toFile()))) {
            props.store(bos, "P2P File Sharing - Local AI Provider Configuration");
        } catch (IOException e) {
            LOG.log(Level.FINE, "Lỗi lưu file cấu hình AI", e);
        }
    }
}
