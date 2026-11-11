package com.nhannv.p2p.network;

import com.nhannv.p2p.model.SharedFile;
import javafx.concurrent.Task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Trình điều phối tải tệp trung tâm (Download Manager) cho Milestone 4.
 *
 * <h2>Vai trò kiến trúc</h2>
 * <ul>
 *   <li>Tự động phân tích số lượng Peer đang giữ tệp có cùng mã {@code sha256Hash}:
 *     <ul>
 *       <li>Nếu <b>{@code N > 1} Peers</b>: Khởi chạy {@link SwarmDownloadTask} (Tải đa nguồn song song).</li>
 *       <li>Nếu <b>{@code 1} Peer</b>: Khởi chạy {@link ResumableDownloadTask} (Tải đơn nguồn có hỗ trợ Pause/Resume).</li>
 *     </ul>
 *   </li>
 *   <li>Quản lý vòng đời <b>Tạm dừng (Pause)</b>, <b>Tiếp tục (Resume)</b> và <b>Hủy (Cancel)</b>
 *       cho phiên tải hiện hành mà tầng UI không cần chạm vào chi tiết Socket hay file {@code .part}.</li>
 * </ul>
 */
public class DownloadManager {

    private final Consumer<String> eventLogger;

    private SharedFile lastRequestedFile;
    private List<String> lastTargetIps;
    private long[] savedSwarmPartProgress;

    private ResumableDownloadTask activeResumableTask;
    private SwarmDownloadTask activeSwarmTask;

    public DownloadManager(Consumer<String> eventLogger) {
        this.eventLogger = eventLogger;
    }

    /**
     * Bắt đầu tải mới (hoặc tiếp tục tải nếu phát hiện tệp {@code .part} sẵn có) cho một {@link SharedFile}.
     *
     * @param sharedFile tệp được chọn trên bảng
     * @return {@link Task} tải tệp để {@code MainController} bind vào {@code ProgressBar} và {@code Label}
     */
    public synchronized Task<Boolean> startDownload(SharedFile sharedFile) {
        Objects.requireNonNull(sharedFile, "sharedFile");

        // Nếu đang chuyển sang tải một tệp khác, đặt lại tiến độ Swarm cũ.
        if (lastRequestedFile == null || !lastRequestedFile.sha256Hash().equals(sharedFile.sha256Hash())) {
            savedSwarmPartProgress = null;
        }

        this.lastRequestedFile = sharedFile;
        this.lastTargetIps = sharedFile.resolvedTargetIps();

        return launchTaskInternal();
    }

    /**
     * Tạm dừng (Pause) phiên tải đang chạy, bảo toàn nguyên vẹn tệp {@code <fileName>.part}.
     */
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

    /**
     * Tiếp tục (Resume) phiên tải đã tạm dừng hoặc bị ngắt kết nối giữa chừng.
     */
    public synchronized Task<Boolean> resumeCurrentDownload() {
        if (lastRequestedFile == null || lastTargetIps == null || lastTargetIps.isEmpty()) {
            return null;
        }
        if (isDownloading()) {
            return null;
        }
        return launchTaskInternal();
    }

    /**
     * Hủy hẳn phiên tải hiện tại và xóa sạch tệp tạm {@code <fileName>.part} trên ổ cứng.
     */
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
        if (lastTargetIps.size() > 1) {
            activeResumableTask = null;
            activeSwarmTask = new SwarmDownloadTask(
                    lastRequestedFile,
                    lastTargetIps,
                    savedSwarmPartProgress,
                    eventLogger
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
            String targetIp = lastTargetIps.get(0);
            activeResumableTask = new ResumableDownloadTask(lastRequestedFile, targetIp, eventLogger);
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
                // Bỏ qua nếu file đang được đóng.
            }
        }
    }
}
