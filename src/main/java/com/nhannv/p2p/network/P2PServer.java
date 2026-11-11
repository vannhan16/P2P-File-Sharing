package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.util.HashUtil;
import com.nhannv.p2p.util.SharedFolderManager;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Máy chủ TCP Đa Lệnh (Multi-Command P2P TCP Server) nâng cấp cho Milestone 5:
 * Hỗ trợ xác thực <b>Security PIN (4 chữ số)</b>, kết nối <b>Host:Port (Ngrok/Playit.gg)</b>,
 * truyền phân đoạn {@code RandomAccessFile.seek()}, đo tốc độ cho biểu đồ {@code AreaChart}
 * và ghi nhận lịch sử thực nghiệm {@link TransferHistoryItem}.
 */
public class P2PServer extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(P2PServer.class.getName());
    private static final DateTimeFormatter HISTORY_TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Cổng TCP mặc định cho mọi lệnh P2P. */
    public static final int DEFAULT_PORT = 8889;

    /** Các mã lệnh (Command Header) chuẩn của giao thức P2P. */
    public static final String CMD_PING = "PING";
    public static final String CMD_LIST_FILES = "LIST_FILES";
    public static final String CMD_DOWNLOAD_FILE = "DOWNLOAD_FILE";
    public static final String CMD_DOWNLOAD_CHUNK = "DOWNLOAD_CHUNK";
    public static final String CMD_PUSH_FILE = "PUSH_FILE";

    /** Các hằng số phản hồi xác thực Security PIN. */
    public static final String STATUS_OK = "OK";
    public static final String STATUS_AUTH_FAILED = "AUTH_FAILED";
    public static final long CODE_AUTH_FAILED = -401L;

    /** Kích thước mỗi khối (Chunk size) đọc/ghi mạng và đĩa = 64 KB. */
    public static final int CHUNK_BUFFER_SIZE = 64 * 1024;

    /** Thư mục mặc định lưu các tệp tải về hoặc được Push tới. */
    public static final String DOWNLOADS_DIR_NAME = "Downloads";

    private static final int SOCKET_TIMEOUT_MS = 15_000;
    private static final int LIST_CONNECT_TIMEOUT_MS = 3_500;

    private final int port;
    private final SharedFolderManager sharedFolderManager;
    private final Path downloadDirectory;
    private final Consumer<SharedFile> onFileReceivedCallback;
    private final Consumer<String> eventLogger;
    private DiscoveryService discoveryService;
    private Consumer<TransferHistoryItem> historyCallback;
    private DoubleConsumer speedSampleCallback;

    /** Cấu hình Security PIN (4 chữ số) của máy chủ. */
    private volatile boolean securityPinEnabled = false;
    private volatile String securityPinCode = "";

    /** Thread Pool xử lý đồng thời nhiều Peer cùng truy vấn danh sách hoặc tải các phân đoạn. */
    private final ExecutorService clientWorkerPool;
    private volatile ServerSocket serverSocket;

    public P2PServer(SharedFolderManager sharedFolderManager, Consumer<SharedFile> onFileReceivedCallback) {
        this(DEFAULT_PORT, sharedFolderManager, onFileReceivedCallback, null);
    }

    public P2PServer(
            int port,
            SharedFolderManager sharedFolderManager,
            Consumer<SharedFile> onFileReceivedCallback
    ) {
        this(port, sharedFolderManager, onFileReceivedCallback, null);
    }

    public P2PServer(
            int port,
            SharedFolderManager sharedFolderManager,
            Consumer<SharedFile> onFileReceivedCallback,
            Consumer<String> eventLogger
    ) {
        this.port = port;
        this.sharedFolderManager = Objects.requireNonNull(sharedFolderManager, "sharedFolderManager");
        this.downloadDirectory = Path.of(DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.onFileReceivedCallback = onFileReceivedCallback;
        this.eventLogger = eventLogger;

        AtomicInteger threadCounter = new AtomicInteger(1);
        this.clientWorkerPool = Executors.newCachedThreadPool(runnable -> {
            Thread t = new Thread(runnable, "p2p-server-worker-" + threadCounter.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    /** Cập nhật cấu hình bật/tắt mã PIN bảo vệ 4 số từ UI. */
    public void updateSecurityPinConfig(boolean enabled, String pinCode) {
        this.securityPinEnabled = enabled;
        this.securityPinCode = (pinCode == null) ? "" : pinCode.trim();
    }

    public boolean isSecurityPinEnabled() {
        return securityPinEnabled;
    }

    public String getSecurityPinCode() {
        return securityPinCode;
    }

    /** Kiểm tra mã PIN do Client gửi lên có hợp lệ với cấu hình hiện tại hay không. */
    public boolean isPinAuthorized(String clientPin) {
        if (!securityPinEnabled || securityPinCode.isEmpty()) {
            return true;
        }
        return securityPinCode.equals(clientPin == null ? "" : clientPin.trim());
    }

    public void setDiscoveryService(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    public void setHistoryCallback(Consumer<TransferHistoryItem> historyCallback) {
        this.historyCallback = historyCallback;
    }

    public void setSpeedSampleCallback(DoubleConsumer speedSampleCallback) {
        this.speedSampleCallback = speedSampleCallback;
    }

    @Override
    protected Void call() {
        try {
            Files.createDirectories(downloadDirectory);

            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            this.serverSocket = ss;

            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Sẵn sàng (P2P Server đang lắng nghe trên TCP port " + port + ")");
            LOG.info(() -> "P2PServer (Multi-Command) đã khởi động trên TCP port " + port);

            while (!isCancelled() && !ss.isClosed()) {
                try {
                    Socket clientSocket = ss.accept();
                    clientWorkerPool.submit(() -> handleClientConnection(clientSocket));
                } catch (SocketException e) {
                    if (isCancelled() || ss.isClosed()) {
                        break;
                    }
                    LOG.log(Level.WARNING, "Lỗi Socket khi chờ kết nối P2P", e);
                } catch (IOException e) {
                    LOG.log(Level.WARNING, "Lỗi I/O khi chấp nhận kết nối P2P", e);
                }
            }
        } catch (BindException e) {
            LOG.log(Level.SEVERE, "Cổng TCP " + port + " đã bị chiếm dụng", e);
            updateMessage("Trạng thái: Lỗi - Cổng TCP " + port + " đang bị sử dụng bởi tiến trình khác!");
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Không thể khởi tạo P2PServer", e);
            updateMessage("Trạng thái: Lỗi khởi tạo P2PServer - " + e.getMessage());
        } finally {
            stopServer();
        }
        return null;
    }

    private void handleClientConnection(Socket clientSocket) {
        String peerIp = clientSocket.getInetAddress().getHostAddress();
        try (Socket socket = clientSocket) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream(), CHUNK_BUFFER_SIZE));
            DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream(), CHUNK_BUFFER_SIZE));

            String command = dis.readUTF();

            switch (command) {
                case CMD_PING -> handlePingCommand(dis, dos, peerIp);
                case CMD_LIST_FILES -> handleListFilesCommand(dos);
                case CMD_DOWNLOAD_FILE -> handleDownloadFileCommand(dis, dos, peerIp);
                case CMD_DOWNLOAD_CHUNK -> handleDownloadChunkCommand(dis, dos, peerIp);
                case CMD_PUSH_FILE -> handlePushFileCommand(dis, dos, peerIp);
                default -> handleLegacyPushFile(command, dis, peerIp);
            }
        } catch (EOFException e) {
            LOG.log(Level.FINE, "Peer " + peerIp + " đã đóng luồng kết nối", e);
        } catch (IOException e) {
            LOG.log(Level.FINE, "Kết thúc phiên giao tiếp với Peer " + peerIp + ": " + e.getMessage());
        }
    }

    private void handlePingCommand(DataInputStream dis, DataOutputStream dos, String senderIp) throws IOException {
        String callerHostName = dis.readUTF();
        if (discoveryService != null) {
            discoveryService.registerPeerFromPing(senderIp, callerHostName);
        }
        String myHost;
        try {
            myHost = java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            String env = System.getenv("COMPUTERNAME");
            myHost = (env != null && !env.isBlank()) ? env : "P2P-Node";
        }
        dos.writeUTF(myHost);
        dos.writeBoolean(securityPinEnabled);
        dos.flush();
    }

    private void handleListFilesCommand(DataOutputStream dos) throws IOException {
        List<SharedFile> files = sharedFolderManager.getSharedFiles();
        dos.writeInt(files.size());
        for (SharedFile sf : files) {
            dos.writeUTF(sf.toWireLine());
        }
        dos.flush();
    }

    private void handleDownloadFileCommand(DataInputStream dis, DataOutputStream dos, String requesterIp)
            throws IOException {
        String requestedFileName = dis.readUTF();
        String clientPin = dis.readUTF();

        if (!isPinAuthorized(clientPin)) {
            dos.writeLong(CODE_AUTH_FAILED);
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            logServerEvent("CẢNH BÁO BẢO MẬT: Từ chối DOWNLOAD_FILE \"" + requestedFileName
                    + "\" từ " + requesterIp + " (AUTH_FAILED - Sai mã PIN)!");
            return;
        }

        File fileOnDisk = sharedFolderManager.resolveSharedFileOnDisk(requestedFileName);
        if (fileOnDisk == null || !fileOnDisk.exists()) {
            dos.writeLong(-1L);
            dos.flush();
            return;
        }

        SharedFile meta = sharedFolderManager.findMetadataByName(fileOnDisk.getName());
        String sha256 = (meta != null) ? meta.sha256Hash() : HashUtil.calculateSHA256(fileOnDisk);
        long fileSize = fileOnDisk.length();

        dos.writeLong(fileSize);
        dos.writeUTF(sha256);

        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long startNano = System.nanoTime();
        long totalSent = 0L;

        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(fileOnDisk), CHUNK_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1 && !isCancelled()) {
                dos.write(buffer, 0, bytesRead);
                totalSent += bytesRead;
                double speed = calculateSpeedMBps(totalSent, startNano);
                reportSpeedSample(speed);
            }
        }
        dos.flush();

        long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
        double avgSpeed = calculateSpeedMBps(totalSent, startNano);
        reportSpeedSample(0.0);
        logServerEvent("Đã phục vụ tệp \"" + fileOnDisk.getName() + "\" cho Peer " + requesterIp);
        recordHistory(fileOnDisk.getName(), fileSize, requesterIp, "Gửi (Serve Pull)", avgSpeed, durationMs, "Hợp lệ");
    }

    /**
     * Xử lý lệnh {@code "DOWNLOAD_CHUNK"} có kiểm tra mã PIN bảo mật 4 chữ số.
     */
    private void handleDownloadChunkCommand(DataInputStream dis, DataOutputStream dos, String requesterIp)
            throws IOException {
        String requestedFileName = dis.readUTF();
        long startOffset = dis.readLong();
        long requestedLength = dis.readLong();
        String clientPin = dis.readUTF();

        if (!isPinAuthorized(clientPin)) {
            dos.writeLong(CODE_AUTH_FAILED);
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            logServerEvent("CẢNH BÁO BẢO MẬT: Từ chối DOWNLOAD_CHUNK \"" + requestedFileName
                    + "\" từ " + requesterIp + " (AUTH_FAILED - Sai mã PIN)!");
            return;
        }

        File fileOnDisk = sharedFolderManager.resolveSharedFileOnDisk(requestedFileName);
        if (fileOnDisk == null || !fileOnDisk.exists()) {
            dos.writeLong(-1L);
            dos.flush();
            return;
        }

        long totalFileSize = fileOnDisk.length();
        if (startOffset < 0 || startOffset > totalFileSize || requestedLength < 0) {
            dos.writeLong(-1L);
            dos.flush();
            return;
        }

        long actualLength = Math.min(requestedLength, totalFileSize - startOffset);

        SharedFile meta = sharedFolderManager.findMetadataByName(fileOnDisk.getName());
        String sha256 = (meta != null) ? meta.sha256Hash() : HashUtil.calculateSHA256(fileOnDisk);

        dos.writeLong(totalFileSize);
        dos.writeLong(actualLength);
        dos.writeUTF(sha256);

        logServerEvent(String.format(Locale.US,
                "Phục vụ DOWNLOAD_CHUNK \"%s\" cho %s [offset=%d, length=%d bytes]",
                fileOnDisk.getName(), requesterIp, startOffset, actualLength));

        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long startNano = System.nanoTime();
        long sentBytes = 0L;

        try (RandomAccessFile raf = new RandomAccessFile(fileOnDisk, "r")) {
            raf.seek(startOffset);

            long remaining = actualLength;
            while (remaining > 0 && !isCancelled()) {
                int maxToRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = raf.read(buffer, 0, maxToRead);
                if (bytesRead == -1) {
                    break;
                }
                dos.write(buffer, 0, bytesRead);
                remaining -= bytesRead;
                sentBytes += bytesRead;

                double speed = calculateSpeedMBps(sentBytes, startNano);
                reportSpeedSample(speed);
            }
        }
        dos.flush();

        long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
        double avgSpeed = calculateSpeedMBps(sentBytes, startNano);
        reportSpeedSample(0.0);
        if (startOffset + actualLength >= totalFileSize) {
            recordHistory(fileOnDisk.getName(), actualLength, requesterIp, "Gửi (Serve Chunk)", avgSpeed, durationMs, "Hợp lệ");
        }
    }

    /**
     * Xử lý lệnh {@code "PUSH_FILE"} kèm xác thực mã PIN bảo mật.
     */
    private void handlePushFileCommand(DataInputStream dis, DataOutputStream dos, String senderIp)
            throws IOException {
        String rawFileName = dis.readUTF();
        long totalBytes = dis.readLong();
        String expectedSha256 = dis.readUTF();
        String clientPin = dis.readUTF();

        if (!isPinAuthorized(clientPin)) {
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            logServerEvent("CẢNH BÁO BẢO MẬT: Từ chối nhận PUSH_FILE \"" + rawFileName
                    + "\" từ " + senderIp + " (AUTH_FAILED - Sai mã PIN)!");
            return;
        }

        dos.writeUTF(STATUS_OK);
        dos.flush();
        receivePushedStream(rawFileName, totalBytes, expectedSha256, dis, senderIp);
    }

    private void handleLegacyPushFile(String rawFileName, DataInputStream dis, String senderIp)
            throws IOException {
        if (!isPinAuthorized("")) {
            logServerEvent("CẢNH BÁO BẢO MẬT: Từ chối kết nối cũ không có mã PIN từ " + senderIp);
            return;
        }
        long totalBytes = dis.readLong();
        receivePushedStream(rawFileName, totalBytes, "", dis, senderIp);
    }

    private void receivePushedStream(
            String rawFileName,
            long totalBytes,
            String expectedSha256,
            DataInputStream dis,
            String senderIp
    ) throws IOException {
        if (totalBytes < 0) {
            throw new IOException("Kích thước tệp không hợp lệ: " + totalBytes);
        }

        String safeFileName = Path.of(rawFileName).getFileName().toString();
        File outputFile = resolveOutputFile(downloadDirectory, safeFileName);

        updateProgress(0, Math.max(totalBytes, 1));
        updateMessage("Trạng thái: Đang nhận \"" + outputFile.getName() + "\" từ " + senderIp + "...");
        logServerEvent("Đang nhận tệp Push \"" + outputFile.getName() + "\" từ Peer " + senderIp);

        MessageDigest digest = HashUtil.newSha256Digest();
        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long totalBytesRead = 0;
        long startNano = System.nanoTime();

        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(outputFile), CHUNK_BUFFER_SIZE)) {

            long remaining = totalBytes;
            while (remaining > 0 && !isCancelled()) {
                int maxToRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = dis.read(buffer, 0, maxToRead);
                if (bytesRead == -1) {
                    break;
                }
                bos.write(buffer, 0, bytesRead);
                digest.update(buffer, 0, bytesRead);

                totalBytesRead += bytesRead;
                remaining -= bytesRead;

                updateProgress(totalBytesRead, Math.max(totalBytes, 1));
                double speedMbps = calculateSpeedMBps(totalBytesRead, startNano);
                reportSpeedSample(speedMbps);
                updateMessage(String.format(Locale.US,
                        "Trạng thái: Đang nhận \"%s\" từ %s (%.2f MB/s)",
                        outputFile.getName(), senderIp, speedMbps));
            }
            bos.flush();
        }

        long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
        double avgSpeed = calculateSpeedMBps(totalBytesRead, startNano);
        reportSpeedSample(0.0);

        if (totalBytesRead != totalBytes) {
            Files.deleteIfExists(outputFile.toPath());
            recordHistory(safeFileName, totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs, "Lỗi (Ngắt kết nối)");
            throw new EOFException("Tệp nhận chưa hoàn chỉnh (" + totalBytesRead + "/" + totalBytes + " bytes)");
        }

        String actualSha256 = HashUtil.toHexString(digest.digest());
        if (!expectedSha256.isBlank() && !expectedSha256.equalsIgnoreCase(actualSha256)) {
            Files.deleteIfExists(outputFile.toPath());
            updateProgress(0.0, 1.0);
            updateMessage("Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!");
            logServerEvent("CẢNH BÁO: Tệp Push \"" + outputFile.getName() + "\" sai lệch SHA-256 -> Đã xóa!");
            recordHistory(safeFileName, totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs, "Lỗi SHA-256");
            return;
        }

        updateProgress(1.0, 1.0);
        updateMessage("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");
        logServerEvent("Đã nhận hoàn tất \"" + outputFile.getName()
                + "\" | SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");
        recordHistory(outputFile.getName(), totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs,
                "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");

        if (onFileReceivedCallback != null) {
            onFileReceivedCallback.accept(new SharedFile(
                    outputFile.getName(),
                    outputFile.length(),
                    actualSha256,
                    senderIp,
                    senderIp
            ));
        }
    }

    /**
     * Gửi lệnh {@code "LIST_FILES"} tới một {@link Peer} (hỗ trợ {@code peer.tcpPort()} động cho Ngrok/Playit).
     */
    public static List<SharedFile> requestFileListFromPeer(Peer peer) throws IOException {
        Objects.requireNonNull(peer, "peer");
        return requestFileListFromPeer(peer, peer.tcpPort());
    }

    public static List<SharedFile> requestFileListFromPeer(Peer peer, int port) throws IOException {
        Objects.requireNonNull(peer, "peer");
        String endpoint = (port == DEFAULT_PORT) ? peer.ipAddress() : (peer.ipAddress() + ":" + port);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.ipAddress(), port), LIST_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);

            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(CMD_LIST_FILES);
                dos.flush();

                int count = dis.readInt();
                if (count <= 0 || count > 10_000) {
                    return List.of();
                }

                List<SharedFile> files = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String line = dis.readUTF();
                    SharedFile sf = SharedFile.fromWireLine(line, endpoint);
                    if (sf != null) {
                        files.add(new SharedFile(
                                sf.fileName(),
                                sf.fileSize(),
                                sf.sha256Hash(),
                                endpoint,
                                peer.hostName(),
                                List.of(endpoint)
                        ));
                    }
                }
                return files;
            }
        }
    }

    public static File resolveOutputFile(Path directory, String fileName) {
        Path candidate = directory.resolve(fileName);
        if (!Files.exists(candidate)) {
            return candidate.toFile();
        }
        int dotIndex = fileName.lastIndexOf('.');
        String baseName = (dotIndex > 0) ? fileName.substring(0, dotIndex) : fileName;
        String extension = (dotIndex > 0) ? fileName.substring(dotIndex) : "";
        int counter = 1;
        while (Files.exists(candidate)) {
            candidate = directory.resolve(baseName + " (" + counter + ")" + extension);
            counter++;
        }
        return candidate.toFile();
    }

    private void logServerEvent(String message) {
        if (eventLogger != null) {
            eventLogger.accept(message);
        }
    }

    private void reportSpeedSample(double speedMBps) {
        DoubleConsumer cb = this.speedSampleCallback;
        if (cb != null) {
            cb.accept(speedMBps);
        }
    }

    private void recordHistory(
            String fileName,
            long sizeBytes,
            String peerEndpoint,
            String direction,
            double avgSpeedMBps,
            long durationMs,
            String shaStatus
    ) {
        Consumer<TransferHistoryItem> cb = this.historyCallback;
        if (cb != null) {
            cb.accept(new TransferHistoryItem(
                    LocalDateTime.now().format(HISTORY_TIME_FMT),
                    fileName,
                    sizeBytes,
                    peerEndpoint,
                    direction,
                    avgSpeedMBps,
                    durationMs,
                    shaStatus
            ));
        }
    }

    private static double calculateSpeedMBps(long bytesTransferred, long startTimeNano) {
        double elapsedSeconds = (System.nanoTime() - startTimeNano) / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0001) {
            return 0.0;
        }
        return (bytesTransferred / (1024.0 * 1024.0)) / elapsedSeconds;
    }

    public void stopServer() {
        cancel(true);
        ServerSocket ss = this.serverSocket;
        if (ss != null && !ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException ignored) {
            }
        }
        clientWorkerPool.shutdownNow();
        try {
            clientWorkerPool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
