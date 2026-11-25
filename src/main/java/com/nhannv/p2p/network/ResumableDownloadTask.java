package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.util.BandwidthLimiter;
import com.nhannv.p2p.util.HashUtil;
import com.nhannv.p2p.util.PathUtil;
import com.nhannv.p2p.util.TransferUtil;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp hỗ trợ <b>Pause / Resume</b> dựa trên tệp tạm {@code .part}
 * và tệp siêu dữ liệu sidecar {@code .part.meta} (Milestone 6).
 *
 * <h2>Các Cải tiến Quan trọng</h2>
 * <ul>
 *   <li><b>Phân loại kết quả rõ ràng (B10):</b> Trả về {@link DownloadResult} thay vì boolean.</li>
 *   <li><b>Sidecar Metadata thống nhất (B1):</b> Dùng {@link DownloadPartMeta} đồng bộ với Swarm.</li>
 *   <li><b>Chống nối sai khi tệp nguồn bị đổi (B3):</b> Kiểm tra khớp SHA-256 và kích thước tệp nguồn.</li>
 *   <li><b>Xác thực Challenge-Response (S1):</b> Mã PIN không gửi qua mạng.</li>
 *   <li><b>Tiết kiệm I/O đĩa (P2-4):</b> Tính mã băm on-the-fly trong lúc ghi.</li>
 * </ul>
 */
public class ResumableDownloadTask extends Task<DownloadResult> {

    private static final Logger LOG = Logger.getLogger(ResumableDownloadTask.class.getName());

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;
    private static final long UI_THROTTLE_MS = 60L; // Giới hạn tần suất update UI ~16 FPS

    private final SharedFile targetFile;
    private final String targetHost;
    private final int targetPort;
    private final String authPin;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;
    private final DoubleConsumer speedSampleCallback;
    private final Consumer<TransferHistoryItem> historyCallback;
    private final DownloadManager.ChunkMatrixCallback chunkMatrixCallback;

    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;

    private long lastUiUpdateMillis = 0L;

    public ResumableDownloadTask(SharedFile targetFile, String targetEndpoint, Consumer<String> eventLogger) {
        this(targetFile, targetEndpoint, "", eventLogger, null, null, null);
    }

    public ResumableDownloadTask(
            SharedFile targetFile,
            String targetEndpoint,
            String authPin,
            Consumer<String> eventLogger,
            DoubleConsumer speedSampleCallback,
            Consumer<TransferHistoryItem> historyCallback,
            DownloadManager.ChunkMatrixCallback chunkMatrixCallback
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        Peer.HostPort hp = Peer.parseHostPort(targetEndpoint, P2PServer.DEFAULT_PORT);
        this.targetHost = hp.host();
        this.targetPort = hp.port();
        this.authPin = (authPin == null) ? "" : authPin.trim();
        this.downloadDirectory = Path.of(P2PServer.DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.eventLogger = eventLogger;
        this.speedSampleCallback = speedSampleCallback;
        this.historyCallback = historyCallback;
        this.chunkMatrixCallback = chunkMatrixCallback;
    }

    public void pauseDownload() {
        this.isPaused = true;
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void cancelAndDeletePart() {
        this.isCanceledByUser = true;
        this.isPaused = false;
        cancel(true);
    }

    public Path getPartFilePath() {
        String safeFileName = PathUtil.sanitizeFileName(targetFile.fileName());
        return downloadDirectory.resolve(safeFileName + ".part");
    }

    public Path getPartMetaFilePath() {
        String safeFileName = PathUtil.sanitizeFileName(targetFile.fileName());
        return downloadDirectory.resolve(safeFileName + ".part.meta");
    }

    @Override
    protected DownloadResult call() {
        String safeFileName = PathUtil.sanitizeFileName(targetFile.fileName());
        File partFile = getPartFilePath().toFile();
        Path metaPath = getPartMetaFilePath();
        long expectedTotalSize = targetFile.fileSize();
        String endpointStr = targetPort == P2PServer.DEFAULT_PORT ? targetHost : (targetHost + ":" + targetPort);
        long startNanoOverall = System.nanoTime();
        long sessionBytesRead = 0L;

        try {
            Files.createDirectories(downloadDirectory);

            // [B1 Fix]: Đọc Sidecar Metadata để biết chính xác số byte và block đã tải
            DownloadPartMeta meta = DownloadPartMeta.loadOrCreate(metaPath, targetFile.sha256Hash(), expectedTotalSize);
            long existingBytes = meta.getDownloadedBytes();

            // Nếu file .part bị mất hoặc kích thước đĩa nhỏ hơn meta, đồng bộ lại
            if (!partFile.exists() || partFile.length() < existingBytes) {
                existingBytes = partFile.exists() ? partFile.length() : 0L;
            }

            // Đánh dấu lên Bản đồ 32 mảnh (BitTorrent Matrix)
            updateChunkMatrixProgress(existingBytes, Math.max(expectedTotalSize, 1L), false);

            if (existingBytes > 0 && existingBytes < expectedTotalSize) {
                double resumedPercent = (existingBytes * 100.0) / expectedTotalSize;
                logEvent(String.format(Locale.US,
                        "Tiếp tục tải (Resume) \"%s\" từ offset %d (%.1f%%)...",
                        safeFileName, existingBytes, resumedPercent));
            } else if (existingBytes == 0) {
                logEvent(String.format(Locale.US,
                        "Bắt đầu tải \"%s\" (%s) từ Peer %s...",
                        safeFileName, targetFile.formattedSize(), endpointStr));
            }

            long bytesRemainingToRequest = Math.max(expectedTotalSize - existingBytes, 0L);
            String serverSha256 = targetFile.sha256Hash();

            if (bytesRemainingToRequest > 0 || expectedTotalSize == 0) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(targetHost, targetPort), CONNECT_TIMEOUT_MS);
                    socket.setSoTimeout(READ_TIMEOUT_MS);
                    socket.setTcpNoDelay(true);

                    try (DataOutputStream dos = new DataOutputStream(
                            new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                         DataInputStream dis = new DataInputStream(
                                 new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                        // Gửi yêu cầu DOWNLOAD_CHUNK
                        dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                        dos.writeUTF(safeFileName);
                        dos.writeLong(existingBytes);
                        dos.writeLong(bytesRemainingToRequest);
                        dos.flush();

                        // [S1 Fix]: Xác thực Challenge-Response HMAC
                        if (!P2PServer.performClientAuth(dis, dos, authPin, P2PServer.CMD_DOWNLOAD_CHUNK)) {
                            updateProgress(0.0, 1.0);
                            String msg = "Từ chối truy cập (AUTH_FAILED): Sai mã PIN bảo mật của Peer " + endpointStr + "!";
                            updateMessage("Trạng thái: " + msg);
                            logEvent(msg);
                            TransferUtil.recordHistory(historyCallback, safeFileName, expectedTotalSize, endpointStr, "Nhận (Pull)", 0.0, 1L, "Lỗi: Sai mã PIN");
                            return DownloadResult.AUTH_FAILED;
                        }

                        long remoteTotalSize = dis.readLong();
                        if (remoteTotalSize < 0) {
                            updateProgress(0.0, 1.0);
                            String msg = "Lỗi: Tệp \"" + safeFileName + "\" không tồn tại trên Peer " + endpointStr;
                            updateMessage("Trạng thái: " + msg);
                            logEvent(msg);
                            return DownloadResult.FAILED;
                        }

                        long chunkLengthToRead = dis.readLong();
                        String remoteSha = dis.readUTF();

                        // [B3 Fix]: Kiểm tra nếu tệp trên máy nguồn đã bị sửa đổi
                        if ((serverSha256 != null && !serverSha256.isBlank() && !remoteSha.equalsIgnoreCase(serverSha256))
                                || (expectedTotalSize > 0 && remoteTotalSize != expectedTotalSize)) {
                            Files.deleteIfExists(partFile.toPath());
                            meta.delete();
                            updateProgress(0.0, 1.0);
                            String changeMsg = "CẢNH BÁO: Tệp trên máy nguồn đã bị thay đổi (Khác mã SHA-256 hoặc kích thước)! Đã hủy tải nối tiếp.";
                            updateMessage("Trạng thái: " + changeMsg);
                            logEvent(changeMsg);
                            TransferUtil.recordHistory(historyCallback, safeFileName, expectedTotalSize, endpointStr, "Nhận (Pull)", 0.0, 1L, "Lỗi: File đã đổi trên nguồn");
                            return DownloadResult.FAILED;
                        }

                        if (serverSha256 == null || serverSha256.isBlank()) {
                            serverSha256 = remoteSha;
                        }

                        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                        long currentTotalDownloaded = existingBytes;
                        long sessionStartNano = System.nanoTime();

                        try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                            raf.seek(existingBytes);

                            long remaining = chunkLengthToRead;
                            while (remaining > 0) {
                                if (isPaused) {
                                    reportSpeed(0.0);
                                    updateChunkMatrixProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1L), true);
                                    meta.save();
                                    double pct = remoteTotalSize > 0 ? (currentTotalDownloaded * 100.0) / remoteTotalSize : 0.0;
                                    String pauseMsg = String.format(Locale.US, "Tạm dừng tại %.1f%% (%s / %s)",
                                            pct, SharedFile.formatSize(currentTotalDownloaded), SharedFile.formatSize(remoteTotalSize));
                                    updateMessage("Trạng thái: " + pauseMsg);
                                    logEvent(pauseMsg);
                                    return DownloadResult.PAUSED;
                                }

                                if (isCanceledByUser || isCancelled()) {
                                    reportSpeed(0.0);
                                    raf.close();
                                    Files.deleteIfExists(partFile.toPath());
                                    meta.delete();
                                    updateProgress(0.0, 1.0);
                                    updateMessage("Trạng thái: Đã hủy tải và xóa tệp tạm " + partFile.getName());
                                    logEvent("Đã hủy tải \"" + safeFileName + "\" và xóa file .part.");
                                    return DownloadResult.CANCELLED;
                                }

                                int maxToRead = (int) Math.min(buffer.length, remaining);
                                int read = dis.read(buffer, 0, maxToRead);
                                if (read == -1) {
                                    break;
                                }

                                raf.write(buffer, 0, read);
                                BandwidthLimiter.getInstance().throttle(read);

                                currentTotalDownloaded += read;
                                sessionBytesRead += read;
                                remaining -= read;

                                // Đánh dấu block meta
                                int currentBlockIdx = (int) (currentTotalDownloaded / meta.getBlockSize());
                                meta.markBlockCompleted(currentBlockIdx);

                                // [P2-1 Fix]: Gom tần suất cập nhật UI (~16 lần/s)
                                long nowMs = System.currentTimeMillis();
                                if (nowMs - lastUiUpdateMillis >= UI_THROTTLE_MS) {
                                    lastUiUpdateMillis = nowMs;
                                    updateChunkMatrixProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1L), false);
                                    double speedMBps = TransferUtil.calculateSpeedMBps(sessionBytesRead, sessionStartNano);
                                    reportSpeed(speedMBps);
                                    int pct = remoteTotalSize > 0 ? (int) ((currentTotalDownloaded * 100) / remoteTotalSize) : 100;
                                    updateProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1));
                                    updateMessage(String.format(Locale.US, "Đang tải: %s (%d%% - %.2f MB/s)", safeFileName, pct, speedMBps));
                                }
                            }
                        }

                        meta.save();

                        if (currentTotalDownloaded < remoteTotalSize) {
                            reportSpeed(0.0);
                            updateChunkMatrixProgress(currentTotalDownloaded, Math.max(remoteTotalSize, 1L), true);
                            double pct = (currentTotalDownloaded * 100.0) / remoteTotalSize;
                            String msg = String.format(Locale.US,
                                    "Gián đoạn kết nối tại %.1f%%. Bấm 'Tiếp tục (Resume)' để tải tiếp!", pct);
                            updateMessage("Trạng thái: " + msg);
                            logEvent(msg);
                            return DownloadResult.PAUSED;
                        }
                    }
                }
            }

            reportSpeed(0.0);
            markAllChunksCompleted();
            updateMessage("Đang kiểm tra mã băm SHA-256 cho tệp " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);
            long durationMs = Math.max(1L, (System.nanoTime() - startNanoOverall) / 1_000_000L);
            double avgSpeed = TransferUtil.calculateSpeedMBps(Math.max(sessionBytesRead, expectedTotalSize), startNanoOverall);

            if (serverSha256 != null && !serverSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(serverSha256)) {
                Files.deleteIfExists(partFile.toPath());
                meta.delete();
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err + " [Mong đợi: " + HashUtil.shortHash(serverSha256)
                        + ", Thực tế: " + HashUtil.shortHash(actualSha256) + "]");
                TransferUtil.recordHistory(historyCallback, safeFileName, expectedTotalSize, endpointStr, "Nhận (Pull)", avgSpeed, durationMs, "Lỗi SHA-256");
                return DownloadResult.FAILED;
            }

            File finalFile = PathUtil.resolveSafeOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            meta.delete(); // Tải xong thành công -> xóa file sidecar .part.meta

            updateProgress(1.0, 1.0);
            String okMsg = "Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256)
                    + "] -> " + finalFile.getName();
            updateMessage(okMsg);
            logEvent(okMsg);
            TransferUtil.recordHistory(historyCallback, finalFile.getName(), expectedTotalSize, endpointStr, "Nhận (Pull)", avgSpeed, durationMs,
                    "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");
            return DownloadResult.COMPLETED;

        } catch (IOException e) {
            reportSpeed(0.0);
            if (isPaused) {
                logEvent("Đã tạm dừng tải \"" + safeFileName + "\" tại offset " + partFile.length());
                return DownloadResult.PAUSED;
            }
            if (isCanceledByUser || isCancelled()) {
                return DownloadResult.CANCELLED;
            }
            LOG.log(Level.WARNING, "Lỗi trong ResumableDownloadTask: " + safeFileName, e);
            updateMessage("Trạng thái: Lỗi kết nối tải tệp - " + e.getMessage());
            logEvent("Lỗi tải tệp \"" + safeFileName + "\": " + e.getMessage());
            return DownloadResult.FAILED;
        }
    }

    private void updateChunkMatrixProgress(long currentDownloaded, long totalBytes, boolean isPausedState) {
        if (chunkMatrixCallback == null || totalBytes <= 0) {
            return;
        }
        int totalCells = DownloadManager.MATRIX_CHUNK_COUNT;
        int completedCells = (int) Math.min(totalCells, (currentDownloaded * totalCells) / totalBytes);

        for (int i = 0; i < completedCells; i++) {
            chunkMatrixCallback.onChunkUpdate(i, DownloadManager.CHUNK_STATE_COMPLETED);
        }
        if (completedCells < totalCells) {
            int currentState = isPausedState
                    ? DownloadManager.CHUNK_STATE_PAUSED_OR_RETRY
                    : DownloadManager.CHUNK_STATE_ACTIVE_PRIMARY;
            chunkMatrixCallback.onChunkUpdate(completedCells, currentState);
            for (int i = completedCells + 1; i < totalCells; i++) {
                chunkMatrixCallback.onChunkUpdate(i, DownloadManager.CHUNK_STATE_WAITING);
            }
        }
    }

    private void markAllChunksCompleted() {
        if (chunkMatrixCallback != null) {
            for (int i = 0; i < DownloadManager.MATRIX_CHUNK_COUNT; i++) {
                chunkMatrixCallback.onChunkUpdate(i, DownloadManager.CHUNK_STATE_COMPLETED);
            }
        }
    }

    private void reportSpeed(double speedMBps) {
        if (speedSampleCallback != null) {
            speedSampleCallback.accept(speedMBps);
        }
    }

    private void logEvent(String msg) {
        if (eventLogger != null) {
            eventLogger.accept(msg);
        }
    }
}
