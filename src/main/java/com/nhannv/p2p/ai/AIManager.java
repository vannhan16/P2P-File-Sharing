package com.nhannv.p2p.ai;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bộ điều phối Trí tuệ Nhân tạo trung tâm (<b>AI Strategy Coordinator &amp; SHA-256 Cache Manager</b>).
 *
 * <h2>Kiến trúc Chịu lỗi &amp; Không Chặn Luồng Mạng (Non-Blocking &amp; Graceful Degradation)</h2>
 * <ul>
 *   <li>Mọi tác vụ AI chạy trên một {@link ExecutorService} riêng biệt ({@code p2p-ai-worker-*}) với
 *       độ ưu tiên thấp, tách biệt hoàn toàn với luồng truyền tệp TCP/UDP và luồng giao diện JavaFX.</li>
 *   <li>Tự động tra cứu {@link AICacheStore} theo mã băm {@code sha256Hash} trước khi gọi AI.</li>
 *   <li>Tự động chuyển đổi êm ái theo chuỗi:
 *       {@link GeminiAIService} &rarr; {@link OllamaAIService} &rarr; {@link HeuristicOfflineAIService}.</li>
 * </ul>
 */
public final class AIManager {

    private static final Logger LOG = Logger.getLogger(AIManager.class.getName());
    private static final AIManager INSTANCE = new AIManager();
    private static final String CONFIG_FILE_NAME = ".p2p-ai-config.properties";

    private final AICacheStore cacheStore;
    private final GeminiAIService geminiService;
    private final OllamaAIService ollamaService;
    private final HeuristicOfflineAIService heuristicService;
    private final ExecutorService aiWorkerPool;
    private final Path configFilePath;

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

    /**
     * Cập nhật Gemini API Key lúc chạy và lưu xuống tệp cấu hình cục bộ {@code .p2p-ai-config.properties}.
     */
    public synchronized void configureGeminiApiKey(String apiKey) {
        geminiService.setApiKey(apiKey);
        saveConfigToDisk();
    }

    /**
     * Trả về tên chiến lược AI đang hoạt động để hiển thị lên thanh Top Header Pill.
     */
    public String getActiveProviderBadgeText() {
        if (geminiService.isAvailable()) {
            return "AI: Gemini Cloud";
        }
        if (ollamaService.isAvailable()) {
            return "AI: Ollama Local";
        }
        return "AI: Local Heuristic";
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
        if (geminiService.isAvailable()) {
            try {
                result = geminiService.analyzeFileMetadata(file, sha256Hash, snippet, sensitiveWarning);
            } catch (Exception e) {
                LOG.log(Level.FINE, "GeminiAIService lỗi, chuyển sang chiến lược dự phòng: " + e.getMessage());
            }
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
     * Phân tích tệp bất đồng bộ trên luồng nền {@code p2p-ai-worker}.
     */
    public CompletableFuture<AIMetadata> analyzeFileAsync(File file, String sha256Hash) {
        return CompletableFuture.supplyAsync(() -> analyzeFileWithCache(file, sha256Hash), aiWorkerPool);
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
            if (geminiService.isAvailable()) {
                try {
                    return geminiService.generatePreDownloadPreview(fileName, fileSize, existingTags, snippetText);
                } catch (Exception ignored) {
                }
            }
            if (ollamaService.isAvailable()) {
                try {
                    return ollamaService.generatePreDownloadPreview(fileName, fileSize, existingTags, snippetText);
                } catch (Exception ignored) {
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
            if (geminiService.isAvailable()) {
                try {
                    return geminiService.diagnoseNetworkIssue(context);
                } catch (Exception ignored) {
                }
            }
            if (ollamaService.isAvailable()) {
                try {
                    return ollamaService.diagnoseNetworkIssue(context);
                } catch (Exception ignored) {
                }
            }
            return heuristicService.diagnoseNetworkIssue(context);
        }, aiWorkerPool);
    }

    public void shutdown() {
        aiWorkerPool.shutdownNow();
    }

    private void loadSavedConfig() {
        if (!Files.exists(configFilePath)) {
            return;
        }
        Properties props = new Properties();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(configFilePath.toFile()))) {
            props.load(bis);
            String savedKey = props.getProperty("gemini.apiKey", "").trim();
            if (!savedKey.isEmpty() && !geminiService.isAvailable()) {
                geminiService.setApiKey(savedKey);
            }
            String savedOllamaModel = props.getProperty("ollama.model", "").trim();
            if (!savedOllamaModel.isEmpty()) {
                ollamaService.setModelName(savedOllamaModel);
            }
        } catch (IOException ignored) {
        }
    }

    private void saveConfigToDisk() {
        Properties props = new Properties();
        props.setProperty("gemini.apiKey", geminiService.getApiKey());
        props.setProperty("ollama.model", ollamaService.getModelName());
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(configFilePath.toFile()))) {
            props.store(bos, "P2P File Sharing - Local AI Provider Configuration");
        } catch (IOException ignored) {
        }
    }
}
