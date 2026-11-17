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
 * Trình điều phối tải tệp trung tâm (Download Manager) nâng cấp cho Milestone 6:
 * Hỗ trợ kết nối {@code host:port} (Ngrok/Playit), truyền mã PIN bảo mật 4 chữ số,
 * trực quan hóa <b>Bản đồ 32 mảnh tải (BitTorrent Chunk Matrix)</b>,
 * đo tốc độ cho biểu đồ {@code AreaChart} và ghi nhận lịch sử {@link TransferHistoryItem}.
 */
public class DownloadManager {

    /** Số ô hiển thị trên Bản đồ mảnh tải (BitTorrent Chunk Matrix UI). */
    public static final int MATRIX_CHUNK_COUNT = 32;

    public static final int CHUNK_STATE_WAITING = 0;
    public static final int CHUNK_STATE_ACTIVE_PRIMARY = 1;
    public static final int CHUNK_STATE_ACTIVE_SWARM = 2;
    public static final int CHUNK_STATE_COMPLETED = 3;
    public static final int CHUNK_STATE_PAUSED_OR_RETRY = 4;

    /** Callback cập nhật trạng thái của ô thứ {@code chunkIndex} (0..31) trên UI. */
    @FunctionalInterface
    public interface ChunkMatrixCallback {
        void onChunkUpdate(int chunkIndex, int state);
    }

    private final Consumer<String> eventLogger;
    private DoubleConsumer speedSampleCallback;
    private Consumer<TransferHistoryItem> historyCallback;
    private ChunkMatrixCallback chunkMatrixCallback;
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

    public void setChunkMatrixCallback(ChunkMatrixCallback chunkMatrixCallback) {
        this.chunkMatrixCallback = chunkMatrixCallback;
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
            resetAllChunks(CHUNK_STATE_WAITING);
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
        resetAllChunks(CHUNK_STATE_WAITING);
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

    private void resetAllChunks(int state) {
        if (chunkMatrixCallback != null) {
            for (int i = 0; i < MATRIX_CHUNK_COUNT; i++) {
                chunkMatrixCallback.onChunkUpdate(i, state);
            }
        }
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
                    historyCallback,
                    chunkMatrixCallback
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
                    historyCallback,
                    chunkMatrixCallback
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
