package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import javafx.concurrent.Task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Supplier;

/**
 * Trình điều phối tải tệp trung tâm (Download Manager) nâng cấp cho Milestone 5:
 * Hỗ trợ kết nối {@code host:port} (Ngrok/Playit), truyền mã PIN bảo mật 4 chữ số,
 * đo tốc độ cho biểu đồ {@code AreaChart} và ghi nhận lịch sử {@link TransferHistoryItem}.
 */
public class DownloadManager {

    private final Consumer<String> eventLogger;
    private DoubleConsumer speedSampleCallback;
    private Consumer<TransferHistoryItem> historyCallback;
    private Supplier<String> authPinSupplier = () -> "";

    private SharedFile lastRequestedFile;
    private List<String> lastTargetEndpoints;
    private long[] savedSwarmPartProgress;

    private ResumableDownloadTask activeResumableTask;
    private SwarmDownloadTask activeSwarmTask;

    public DownloadManager(Consumer<String> eventLogger) {
        this.eventLogger = eventLogger;
    }

    public void setSpeedSampleCallback(DoubleConsumer speedSampleCallback) {
        this.speedSampleCallback = speedSampleCallback;
    }

    public void setHistoryCallback(Consumer<TransferHistoryItem> historyCallback) {
        this.historyCallback = historyCallback;
    }

    public void setAuthPinSupplier(Supplier<String> authPinSupplier) {
        if (authPinSupplier != null) {
            this.authPinSupplier = authPinSupplier;
        }
    }

    public synchronized Task<Boolean> startDownload(SharedFile sharedFile) {
        Objects.requireNonNull(sharedFile, "sharedFile");

        if (lastRequestedFile == null || !lastRequestedFile.sha256Hash().equals(sharedFile.sha256Hash())) {
            savedSwarmPartProgress = null;
        }

        this.lastRequestedFile = sharedFile;
        this.lastTargetEndpoints = sharedFile.resolvedTargetIps();

        return launchTaskInternal();
    }

    public synchronized boolean pauseCurrentDownload() {
        if (activeResumableTask != null && activeResumableTask.isRunning()) {
            activeResumableTask.pauseDownload();
            return true;
        }
        if (activeSwarmTask != null && activeSwarmTask.isRunning()) {
            activeSwarmTask.pauseDownload();
            savedSwarmPartProgress = activeSwarmTask.getPartProgressSnapshot();
            return true;
        }
        return false;
    }

    public synchronized Task<Boolean> resumeCurrentDownload() {
        if (lastRequestedFile == null || lastTargetEndpoints == null || lastTargetEndpoints.isEmpty()) {
            return null;
        }
        if (isDownloading()) {
            return null;
        }
        return launchTaskInternal();
    }

    public synchronized void cancelCurrentDownload() {
        if (activeResumableTask != null) {
            Path partPath = activeResumableTask.getPartFilePath();
            activeResumableTask.cancelAndDeletePart();
            activeResumableTask = null;
            deletePartFileQuietly(partPath);
        }
        if (activeSwarmTask != null) {
            Path partPath = activeSwarmTask.getPartFilePath();
            activeSwarmTask.cancelAndDeletePart();
            activeSwarmTask = null;
            deletePartFileQuietly(partPath);
        }
        if (lastRequestedFile != null) {
            String safeName = Path.of(lastRequestedFile.fileName()).getFileName().toString();
            Path fallbackPart = Path.of(P2PServer.DOWNLOADS_DIR_NAME, safeName + ".part").toAbsolutePath();
            deletePartFileQuietly(fallbackPart);
            if (eventLogger != null) {
                eventLogger.accept("Đã hủy tác vụ tải \"" + safeName + "\" và dọn dẹp file .part.");
            }
        }
        savedSwarmPartProgress = null;
    }

    public synchronized boolean isDownloading() {
        return (activeResumableTask != null && activeResumableTask.isRunning())
                || (activeSwarmTask != null && activeSwarmTask.isRunning());
    }

    public synchronized boolean canResume() {
        if (isDownloading() || lastRequestedFile == null) {
            return false;
        }
        String safeName = Path.of(lastRequestedFile.fileName()).getFileName().toString();
        Path partPath = Path.of(P2PServer.DOWNLOADS_DIR_NAME, safeName + ".part").toAbsolutePath();
        return Files.exists(partPath);
    }

    private Task<Boolean> launchTaskInternal() {
        String currentPin = authPinSupplier.get();
        if (lastTargetEndpoints.size() > 1) {
            activeResumableTask = null;
            activeSwarmTask = new SwarmDownloadTask(
                    lastRequestedFile,
                    lastTargetEndpoints,
                    currentPin,
                    savedSwarmPartProgress,
                    eventLogger,
                    speedSampleCallback,
                    historyCallback
            );
            activeSwarmTask.valueProperty().addListener((obs, oldVal, completedOk) -> {
                if (Boolean.TRUE.equals(completedOk)) {
                    savedSwarmPartProgress = null;
                } else if (activeSwarmTask != null) {
                    savedSwarmPartProgress = activeSwarmTask.getPartProgressSnapshot();
                }
            });
            Thread t = new Thread(activeSwarmTask, "p2p-swarm-coordinator");
            t.setDaemon(true);
            t.start();
            return activeSwarmTask;
        } else {
            activeSwarmTask = null;
            String targetEndpoint = lastTargetEndpoints.get(0);
            activeResumableTask = new ResumableDownloadTask(
                    lastRequestedFile,
                    targetEndpoint,
                    currentPin,
                    eventLogger,
                    speedSampleCallback,
                    historyCallback
            );
            Thread t = new Thread(activeResumableTask, "p2p-resumable-downloader");
            t.setDaemon(true);
            t.start();
            return activeResumableTask;
        }
    }

    private static void deletePartFileQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
        }
    }
}
