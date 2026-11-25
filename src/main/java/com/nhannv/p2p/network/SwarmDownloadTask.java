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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tác vụ tải tệp Đa Nguồn Song Song (<b>Multi-source / Swarm Download</b>),
 * nâng cấp toàn diện theo mô hình Dynamic Block Queue (Milestone 6 &amp; P2-3):
 * <ul>
 *   <li><b>Hàng đợi khối dùng chung (Dynamic Block Queue P2-3):</b> Tệp được chia thành các block 1 MB.
 *       Mỗi worker lấy block tiếp theo từ hàng đợi chung; peer nhanh tải nhiều block hơn, peer chậm không kéo lùi tiến độ.</li>
 *   <li><b>Chuyển nguồn tự nhiên (Dynamic Failover B2, P2-3):</b> Nếu kết nối tới một Peer bị ngắt,
 *       block đó lập tức được đẩy lại vào hàng đợi để Peer khác tải thay thế.</li>
 *   <li><b>Kiểm tra tính toàn vẹn từng phân đoạn (B2):</b> Xác thực chặt chẽ {@code actualChunkLen == remainingInBlock}
 *       và {@code remoteTotalSize == totalSize}.</li>
 *   <li><b>Đồng bộ Sidecar .part.meta (B1):</b> Cùng chia sẻ một cấu trúc metadata với {@link ResumableDownloadTask}.</li>
 *   <li><b>Xác thực Challenge-Response HMAC (S1):</b> Không gửi mã PIN qua mạng.</li>
 * </ul>
 */
public class SwarmDownloadTask extends Task<DownloadResult> {

    private static final Logger LOG = Logger.getLogger(SwarmDownloadTask.class.getName());

    private static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 15_000;
    private static final long UI_THROTTLE_MS = 60L;

    private final SharedFile targetFile;
    private final List<String> peerEndpoints;
    private final String authPin;
    private final Path downloadDirectory;
    private final Consumer<String> eventLogger;
    private final DoubleConsumer speedSampleCallback;
    private final Consumer<TransferHistoryItem> historyCallback;
    private final DownloadManager.ChunkMatrixCallback chunkMatrixCallback;

    private volatile boolean isPaused = false;
    private volatile boolean isCanceledByUser = false;
    private volatile ExecutorService swarmExecutor;

    private long lastUiUpdateMillis = 0L;

    public SwarmDownloadTask(
            SharedFile targetFile,
            List<String> peerEndpoints,
            String authPin,
            Consumer<String> eventLogger,
            DoubleConsumer speedSampleCallback,
            Consumer<TransferHistoryItem> historyCallback,
            DownloadManager.ChunkMatrixCallback chunkMatrixCallback
    ) {
        this.targetFile = Objects.requireNonNull(targetFile, "targetFile");
        if (peerEndpoints == null || peerEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Danh sách peerEndpoints không được rỗng");
        }
        this.peerEndpoints = List.copyOf(peerEndpoints);
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
        ExecutorService exec = this.swarmExecutor;
        if (exec != null) {
            exec.shutdownNow();
        }
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
        long totalSize = targetFile.fileSize();
        int peerCount = peerEndpoints.size();
        String peersSummary = peerCount + " Peers " + peerEndpoints;

        try {
            Files.createDirectories(downloadDirectory);

            // [B1 Fix]: Tải hoặc tạo Sidecar Metadata
            DownloadPartMeta meta = DownloadPartMeta.loadOrCreate(metaPath, targetFile.sha256Hash(), totalSize);

            // Cấp phát trước kích thước tệp .part nếu chưa đủ
            try (RandomAccessFile preAllocRaf = new RandomAccessFile(partFile, "rw")) {
                if (preAllocRaf.length() != totalSize) {
                    preAllocRaf.setLength(totalSize);
                }
            }

            long alreadyDownloaded = meta.getDownloadedBytes();
            final AtomicLong totalBytesDownloaded = new AtomicLong(alreadyDownloaded);
            final AtomicLong sessionBytesDownloaded = new AtomicLong(0L);
            final long startNano = System.nanoTime();

            // Khởi tạo trạng thái mảnh đã có sẵn lên BitTorrent Matrix
            updateOverallChunkMatrix(alreadyDownloaded, Math.max(totalSize, 1L));

            logEvent(String.format(Locale.US,
                    "Bắt đầu tải Swarm Block-Queue \"%s\" (%s) từ %d nguồn: %s...",
                    safeFileName, targetFile.formattedSize(), peerCount, peerEndpoints));

            // [P2-3 Fix]: Tạo hàng đợi khối dùng chung
            ConcurrentLinkedQueue<Integer> blockQueue = new ConcurrentLinkedQueue<>(meta.getIncompleteBlocks());

            AtomicInteger workerIdx = new AtomicInteger(1);
            ExecutorService executor = Executors.newFixedThreadPool(peerCount, r -> {
                Thread t = new Thread(r, "p2p-swarm-worker-" + workerIdx.getAndIncrement());
                t.setDaemon(true);
                return t;
            });
            this.swarmExecutor = executor;

            List<Callable<Boolean>> workers = new ArrayList<>(peerCount);
            AtomicBoolean anyFatalAuthError = new AtomicBoolean(false);

            for (int i = 0; i < peerCount; i++) {
                final int peerIndex = i;
                final String initialPeerEndpoint = peerEndpoints.get(i);

                workers.add(() -> runSwarmWorker(
                        peerIndex,
                        initialPeerEndpoint,
                        safeFileName,
                        partFile,
                        meta,
                        blockQueue,
                        totalSize,
                        peerCount,
                        totalBytesDownloaded,
                        sessionBytesDownloaded,
                        startNano,
                        anyFatalAuthError
                ));
            }

            List<Future<Boolean>> futures = executor.invokeAll(workers);
            executor.shutdown();
            executor.awaitTermination(3, TimeUnit.SECONDS);
            reportSpeed(0.0);
            meta.save();

            if (isPaused) {
                long currentDone = totalBytesDownloaded.get();
                double pct = totalSize > 0 ? (currentDone * 100.0) / totalSize : 0.0;
                String pauseMsg = String.format(Locale.US,
                        "Tạm dừng tải đa nguồn tại %.1f%% (%s / %s)",
                        pct, SharedFile.formatSize(currentDone), SharedFile.formatSize(totalSize));
                updateMessage("Trạng thái: " + pauseMsg);
                logEvent(pauseMsg);
                return DownloadResult.PAUSED;
            }

            if (isCanceledByUser || isCancelled()) {
                Files.deleteIfExists(partFile.toPath());
                meta.delete();
                updateProgress(0.0, 1.0);
                updateMessage("Trạng thái: Đã hủy tải đa nguồn và xóa tệp tạm " + partFile.getName());
                logEvent("Đã hủy tải đa nguồn \"" + safeFileName + "\" và xóa file .part.");
                return DownloadResult.CANCELLED;
            }

            if (anyFatalAuthError.get()) {
                return DownloadResult.AUTH_FAILED;
            }

            // Nếu hàng đợi chưa hết (các peer đều bị ngắt kết nối giữa chừng)
            if (!meta.isAllCompleted()) {
                long currentDone = totalBytesDownloaded.get();
                double pct = totalSize > 0 ? (currentDone * 100.0) / totalSize : 0.0;
                String msg = String.format(Locale.US,
                        "Tải đa nguồn bị gián đoạn tại %.1f%%. Bấm 'Tiếp tục (Resume)' để tải tiếp phần còn thiếu!",
                        pct);
                updateMessage("Trạng thái: " + msg);
                logEvent(msg);
                return DownloadResult.PAUSED;
            }

            markAllChunksCompleted();
            updateMessage("Đang xác thực SHA-256 toàn bộ tệp đa nguồn " + partFile.getName() + "...");
            String actualSha256 = HashUtil.calculateSHA256(partFile);
            String expectedSha256 = targetFile.sha256Hash();
            long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
            double avgSpeed = TransferUtil.calculateSpeedMBps(Math.max(sessionBytesDownloaded.get(), totalSize), startNano);

            if (expectedSha256 != null && !expectedSha256.isBlank()
                    && !actualSha256.equalsIgnoreCase(expectedSha256)) {
                Files.deleteIfExists(partFile.toPath());
                meta.delete();
                updateProgress(0.0, 1.0);
                String err = "Cảnh báo: File đa nguồn bị hỏng (Sai lệch SHA-256)!";
                updateMessage(err);
                logEvent(err);
                TransferUtil.recordHistory(historyCallback, safeFileName, totalSize, peersSummary, "Nhận (Swarm)", avgSpeed, durationMs, "Lỗi SHA-256");
                return DownloadResult.FAILED;
            }

            File finalFile = PathUtil.resolveSafeOutputFile(downloadDirectory, safeFileName);
            Files.move(partFile.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            meta.delete();

            updateProgress(1.0, 1.0);
            String okMsg = String.format(Locale.US,
                    "Tải đa nguồn hoàn tất từ %d Peers (%.2f MB/s)! SHA-256 hợp lệ: [%s] -> %s",
                    peerCount, avgSpeed, HashUtil.shortHash(actualSha256), finalFile.getName());
            updateMessage(okMsg);
            logEvent(okMsg);
            TransferUtil.recordHistory(historyCallback, finalFile.getName(), totalSize, peersSummary, "Nhận (Swarm)", avgSpeed, durationMs,
                    "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");
            return DownloadResult.COMPLETED;

        } catch (Exception e) {
            reportSpeed(0.0);
            if (isCanceledByUser || isCancelled()) {
                try {
                    Files.deleteIfExists(partFile.toPath());
                } catch (IOException ignored) {
                }
                return DownloadResult.CANCELLED;
            }
            LOG.log(Level.WARNING, "Lỗi trong SwarmDownloadTask: " + safeFileName, e);
            updateMessage("Trạng thái: Lỗi tải đa nguồn - " + e.getMessage());
            logEvent("Lỗi tải đa nguồn \"" + safeFileName + "\": " + e.getMessage());
            return DownloadResult.FAILED;
        }
    }

    /**
     * Mỗi worker xử lý liên tục các block lấy từ {@code blockQueue} cho đến khi rỗng.
     */
    private Boolean runSwarmWorker(
            int workerIndex,
            String assignedPeer,
            String safeFileName,
            File partFile,
            DownloadPartMeta meta,
            ConcurrentLinkedQueue<Integer> blockQueue,
            long totalFileSize,
            int peerCount,
            AtomicLong totalBytesDownloaded,
            AtomicLong sessionBytesDownloaded,
            long startNano,
            AtomicBoolean fatalAuthError
    ) {
        String currentPeer = assignedPeer;
        int activeState = (workerIndex % 2 == 0)
                ? DownloadManager.CHUNK_STATE_ACTIVE_PRIMARY
                : DownloadManager.CHUNK_STATE_ACTIVE_SWARM;

        while (!isPaused && !isCanceledByUser && !isCancelled() && !blockQueue.isEmpty()) {
            Integer blockIdx = blockQueue.poll();
            if (blockIdx == null) {
                break;
            }

            long blockStart = (long) blockIdx * meta.getBlockSize();
            long blockLength = (blockIdx == meta.getTotalBlocks() - 1)
                    ? (totalFileSize - blockStart)
                    : meta.getBlockSize();

            boolean success = downloadBlockFromPeer(
                    currentPeer,
                    safeFileName,
                    partFile,
                    blockStart,
                    blockLength,
                    totalFileSize,
                    totalBytesDownloaded,
                    sessionBytesDownloaded,
                    startNano,
                    fatalAuthError
            );

            if (success) {
                meta.markBlockCompleted(blockIdx);
                updateCellChunkMatrix(blockIdx, meta.getTotalBlocks(), DownloadManager.CHUNK_STATE_COMPLETED);
            } else {
                // [P2-3 Failover]: Nếu tải thất bại, đẩy block trở lại hàng đợi cho worker khác nhận
                blockQueue.offer(blockIdx);
                updateCellChunkMatrix(blockIdx, meta.getTotalBlocks(), DownloadManager.CHUNK_STATE_PAUSED_OR_RETRY);

                // Thử đổi sang một peer khác trong danh sách
                for (String alt : peerEndpoints) {
                    if (!alt.equalsIgnoreCase(currentPeer)) {
                        currentPeer = alt;
                        break;
                    }
                }
                try {
                    Thread.sleep(200); // Tránh quay vòng CPU quá nhanh khi rớt mạng
                } catch (InterruptedException ignored) {
                    break;
                }
            }
        }
        return true;
    }

    /**
     * Tải một block từ peer cụ thể kèm kiểm tra toàn vẹn chặt chẽ (B2).
     */
    private boolean downloadBlockFromPeer(
            String peerEndpoint,
            String safeFileName,
            File partFile,
            long blockStartOffset,
            long blockLength,
            long totalFileSize,
            AtomicLong totalBytesDownloaded,
            AtomicLong sessionBytesDownloaded,
            long startNano,
            AtomicBoolean fatalAuthError
    ) {
        Peer.HostPort hp = Peer.parseHostPort(peerEndpoint, P2PServer.DEFAULT_PORT);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hp.host(), hp.port()), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));
                 DataInputStream dis = new DataInputStream(
                         new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE))) {

                dos.writeUTF(P2PServer.CMD_DOWNLOAD_CHUNK);
                dos.writeUTF(safeFileName);
                dos.writeLong(blockStartOffset);
                dos.writeLong(blockLength);
                dos.flush();

                if (!P2PServer.performClientAuth(dis, dos, authPin, P2PServer.CMD_DOWNLOAD_CHUNK)) {
                    fatalAuthError.set(true);
                    return false;
                }

                long remoteTotal = dis.readLong();
                // [B2 Fix]: Kiểm tra remoteTotal == totalFileSize
                if (remoteTotal != totalFileSize) {
                    return false;
                }

                long actualChunkLen = dis.readLong();
                dis.readUTF(); // SHA-256 header

                // [B2 Fix]: Kiểm tra nếu actualChunkLen != blockLength thì coi là lỗi và failover
                if (actualChunkLen != blockLength) {
                    return false;
                }

                byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
                try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                    raf.seek(blockStartOffset);

                    long remaining = actualChunkLen;
                    while (remaining > 0) {
                        if (isPaused || isCanceledByUser || isCancelled()) {
                            return false;
                        }
                        int maxToRead = (int) Math.min(buffer.length, remaining);
                        int read = dis.read(buffer, 0, maxToRead);
                        if (read == -1) {
                            return false;
                        }

                        raf.write(buffer, 0, read);
                        BandwidthLimiter.getInstance().throttle(read);

                        remaining -= read;
                        long totalDone = totalBytesDownloaded.addAndGet(read);
                        long sessionDone = sessionBytesDownloaded.addAndGet(read);

                        // [P2-1 Fix]: Throttled UI update
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastUiUpdateMillis >= UI_THROTTLE_MS) {
                            lastUiUpdateMillis = nowMs;
                            double speedMBps = TransferUtil.calculateSpeedMBps(sessionDone, startNano);
                            reportSpeed(speedMBps);
                            updateProgress(totalDone, Math.max(totalFileSize, 1));
                            updateMessage(String.format(Locale.US,
                                    "Đang tải Swarm (%d Peers - %.2f MB/s)",
                                    peerEndpoints.size(), speedMBps));
                        }
                    }
                }
                return true;
            }
        } catch (IOException e) {
            return false;
        }
    }

    private void updateOverallChunkMatrix(long downloadedBytes, long totalBytes) {
        if (chunkMatrixCallback == null || totalBytes <= 0) {
            return;
        }
        int totalCells = DownloadManager.MATRIX_CHUNK_COUNT;
        int completedCells = (int) Math.min(totalCells, (downloadedBytes * totalCells) / totalBytes);
        for (int i = 0; i < completedCells; i++) {
            chunkMatrixCallback.onChunkUpdate(i, DownloadManager.CHUNK_STATE_COMPLETED);
        }
    }

    private void updateCellChunkMatrix(int blockIdx, int totalBlocks, int state) {
        if (chunkMatrixCallback == null || totalBlocks <= 0) {
            return;
        }
        int totalCells = DownloadManager.MATRIX_CHUNK_COUNT;
        int cellIdx = (int) Math.min(totalCells - 1, ((long) blockIdx * totalCells) / totalBlocks);
        chunkMatrixCallback.onChunkUpdate(cellIdx, state);
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
