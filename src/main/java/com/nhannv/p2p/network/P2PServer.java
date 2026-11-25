package com.nhannv.p2p.network;

import com.nhannv.p2p.ai.FileContentExtractor;
import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.util.BandwidthLimiter;
import com.nhannv.p2p.util.CryptoUtil;
import com.nhannv.p2p.util.HashUtil;
import com.nhannv.p2p.util.PathUtil;
import com.nhannv.p2p.util.SharedFolderManager;
import com.nhannv.p2p.util.TransferUtil;

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
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Máy chủ TCP Đa Lệnh (Multi-Command P2P TCP Server).
 * <p>
 * Đã nâng cấp toàn diện theo chuẩn Kỹ sư Bảo mật &amp; Kiến trúc sạch:
 * <ul>
 *   <li><b>Không kế thừa Task&lt;Void&gt; (B9):</b> Hoạt động độc lập như một Service Daemon (Runnable &amp; AutoCloseable).</li>
 *   <li><b>Xác thực Challenge-Response HMAC-SHA256 (S1):</b> Mã PIN không truyền thô qua mạng.</li>
 *   <li><b>Chống Brute-force &amp; Khóa IP (S1):</b> Giới hạn 5 lần sai, khóa 5 phút theo IP.</li>
 *   <li><b>Bảo vệ DoS &amp; Giới hạn tài nguyên (S5):</b> Bounded Thread Pool (16 luồng) và giới hạn 5 kết nối đồng thời/IP.</li>
 *   <li><b>Bảo vệ Push tệp an toàn (B5, B6, S6):</b> Ghi tệp .part trước, cảnh báo mã thực thi, xác nhận hai chiều.</li>
 *   <li><b>Chuẩn hóa tên tệp (S8):</b> Tích hợp {@link PathUtil#sanitizeFileName}.</li>
 * </ul>
 */
public class P2PServer implements Runnable, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(P2PServer.class.getName());

    public static final int DEFAULT_PORT = 8889;

    public static final String CMD_PING = "PING";
    public static final String CMD_LIST_FILES = "LIST_FILES";
    public static final String CMD_DOWNLOAD_FILE = "DOWNLOAD_FILE";
    public static final String CMD_DOWNLOAD_CHUNK = "DOWNLOAD_CHUNK";
    public static final String CMD_PUSH_FILE = "PUSH_FILE";
    public static final String CMD_CHAT_MESSAGE = "CHAT_MESSAGE";

    public static final String STATUS_OK = "OK";
    public static final String STATUS_AUTH_FAILED = "AUTH_FAILED";
    public static final String STATUS_REJECTED = "REJECTED_BY_USER";
    public static final String STATUS_HASH_MISMATCH = "HASH_MISMATCH";
    public static final String STATUS_INSUFFICIENT_SPACE = "INSUFFICIENT_SPACE";
    public static final long CODE_AUTH_FAILED = -401L;
    public static final long CODE_BUSY = -2L;

    public static final int CHUNK_BUFFER_SIZE = 64 * 1024;
    public static final String DOWNLOADS_DIR_NAME = "Downloads";

    private static final int SOCKET_TIMEOUT_MS = 15_000;
    private static final int LIST_CONNECT_TIMEOUT_MS = 3_500;
    private static final int MAX_CONCURRENT_PER_IP = 5;
    private static final int MAX_FAILED_ATTEMPTS = 5;
    private static final long LOCKOUT_DURATION_MS = 5 * 60 * 1000L; // 5 phút

    private static final Set<String> EXECUTABLE_EXTENSIONS = Set.of(
            "exe", "bat", "cmd", "ps1", "jar", "msi", "vbs", "sh"
    );

    private final int port;
    private final SharedFolderManager sharedFolderManager;
    private final Path downloadDirectory;
    private final Consumer<SharedFile> onFileReceivedCallback;
    private final Consumer<String> eventLogger;
    private DiscoveryService discoveryService;
    private Consumer<TransferHistoryItem> historyCallback;
    private DoubleConsumer speedSampleCallback;
    private BiConsumer<String, String> chatMessageCallback;
    private BiFunction<String, Long, Boolean> pushApprovalHandler;
    private Consumer<String> statusMessageCallback;

    private volatile boolean securityPinEnabled = true; // [S1 Fix]: Mặc định bật PIN
    private volatile String securityPinCode = "";
    private volatile boolean askBeforeAcceptingPush = true; // [S6 Fix]: Mặc định bật hỏi xác nhận

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ThreadPoolExecutor clientWorkerPool;
    private volatile ServerSocket serverSocket;

    // Rate limiting & Brute force lock
    private static class AttemptInfo {
        int failedCount = 0;
        long lockedUntil = 0L;
    }
    private final Map<String, AttemptInfo> ipAttempts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeConnectionsPerIp = new ConcurrentHashMap<>();

    public P2PServer(SharedFolderManager sharedFolderManager, Consumer<SharedFile> onFileReceivedCallback) {
        this(DEFAULT_PORT, sharedFolderManager, onFileReceivedCallback, null);
    }

    public P2PServer(int port, SharedFolderManager sharedFolderManager, Consumer<SharedFile> onFileReceivedCallback) {
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

        // Sinh mã PIN bảo mật ngẫu nhiên 6 số bằng SecureRandom
        this.securityPinCode = CryptoUtil.generateSecurePin6();

        // [S5 Fix]: ThreadPool có giới hạn 16 luồng kèm hàng đợi 64 yêu cầu
        AtomicInteger threadCounter = new AtomicInteger(1);
        this.clientWorkerPool = new ThreadPoolExecutor(
                4,
                16,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(64),
                r -> {
                    Thread t = new Thread(r, "p2p-server-worker-" + threadCounter.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

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

    public void setDiscoveryService(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    public void setHistoryCallback(Consumer<TransferHistoryItem> historyCallback) {
        this.historyCallback = historyCallback;
    }

    public void setSpeedSampleCallback(DoubleConsumer speedSampleCallback) {
        this.speedSampleCallback = speedSampleCallback;
    }

    public void setChatMessageCallback(BiConsumer<String, String> chatMessageCallback) {
        this.chatMessageCallback = chatMessageCallback;
    }

    public void setPushApprovalHandler(BiFunction<String, Long, Boolean> pushApprovalHandler) {
        this.pushApprovalHandler = pushApprovalHandler;
    }

    public void setStatusMessageCallback(Consumer<String> statusMessageCallback) {
        this.statusMessageCallback = statusMessageCallback;
    }

    public void setAskBeforeAcceptingPush(boolean askBeforeAcceptingPush) {
        this.askBeforeAcceptingPush = askBeforeAcceptingPush;
    }

    public boolean isAskBeforeAcceptingPush() {
        return askBeforeAcceptingPush;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void setRunning(boolean isRunning) {
        running.set(isRunning);
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try {
            Files.createDirectories(downloadDirectory);

            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            this.serverSocket = ss;

            reportStatus("Sẵn sàng (P2P Server đang lắng nghe trên TCP port " + port + ")");
            LOG.info(() -> "P2PServer đã khởi động trên TCP port " + port);

            while (running.get() && !ss.isClosed()) {
                try {
                    Socket clientSocket = ss.accept();
                    String clientIp = clientSocket.getInetAddress().getHostAddress();

                    // [S5 Fix]: Giới hạn số kết nối đồng thời từ một IP
                    AtomicInteger activeConns = activeConnectionsPerIp.computeIfAbsent(clientIp, k -> new AtomicInteger(0));
                    if (activeConns.incrementAndGet() > MAX_CONCURRENT_PER_IP) {
                        activeConns.decrementAndGet();
                        logServerEvent("CẢNH BÁO: Từ chối kết nối từ " + clientIp + " (Vượt quá giới hạn " + MAX_CONCURRENT_PER_IP + " kết nối đồng thời)");
                        try {
                            clientSocket.close();
                        } catch (IOException ignored) {
                        }
                        continue;
                    }

                    clientWorkerPool.submit(() -> {
                        try {
                            handleClientConnection(clientSocket);
                        } finally {
                            activeConns.decrementAndGet();
                        }
                    });
                } catch (SocketException e) {
                    if (!running.get() || ss.isClosed()) {
                        break;
                    }
                    LOG.log(Level.FINE, "SocketException khi chờ kết nối", e);
                } catch (IOException e) {
                    LOG.log(Level.FINE, "Lỗi I/O khi chấp nhận kết nối", e);
                }
            }
        } catch (BindException e) {
            LOG.log(Level.SEVERE, "Cổng TCP " + port + " đã bị chiếm dụng", e);
            reportStatus("Lỗi: Cổng TCP " + port + " đang bị sử dụng bởi tiến trình khác!");
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Không thể khởi tạo P2PServer", e);
            reportStatus("Lỗi khởi tạo P2PServer: " + e.getMessage());
        } finally {
            stopServer();
        }
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
                case CMD_LIST_FILES -> handleListFilesCommand(dis, dos, peerIp);
                case CMD_DOWNLOAD_FILE -> handleDownloadFileCommand(dis, dos, peerIp, socket);
                case CMD_DOWNLOAD_CHUNK -> handleDownloadChunkCommand(dis, dos, peerIp);
                case CMD_PUSH_FILE -> handlePushFileCommand(dis, dos, peerIp);
                case CMD_CHAT_MESSAGE -> handleChatMessageCommand(dis, dos, peerIp);
                default -> logServerEvent("Từ chối lệnh không xác định: " + command + " từ " + peerIp);
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

    /**
     * [S3 Fix]: Yêu cầu xác thực Challenge-Response cho LIST_FILES khi PIN được bật.
     */
    void handleListFilesCommand(DataInputStream dis, DataOutputStream dos, String requesterIp) throws IOException {
        if (!authenticateClient(dis, dos, requesterIp, CMD_LIST_FILES)) {
            dos.writeInt(0);
            dos.flush();
            return;
        }

        List<SharedFile> files = sharedFolderManager.getSharedFiles();
        dos.writeInt(files.size());
        for (SharedFile sf : files) {
            dos.writeUTF(sf.toWireLine());
        }
        dos.flush();
    }

    private void handleDownloadFileCommand(DataInputStream dis, DataOutputStream dos, String requesterIp, Socket socket)
            throws IOException {
        String requestedFileName = dis.readUTF();

        if (!authenticateClient(dis, dos, requesterIp, CMD_DOWNLOAD_FILE)) {
            dos.writeLong(CODE_AUTH_FAILED);
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            return;
        }

        File fileOnDisk = sharedFolderManager.resolveSharedFileOnDisk(requestedFileName);
        if (fileOnDisk == null || !fileOnDisk.exists()) {
            dos.writeLong(-1L);
            dos.flush();
            return;
        }

        long initialSize = fileOnDisk.length();
        SharedFile meta = sharedFolderManager.findMetadataByName(fileOnDisk.getName());
        String sha256 = (meta != null) ? meta.sha256Hash() : HashUtil.calculateSHA256(fileOnDisk);

        dos.writeLong(initialSize);
        dos.writeUTF(sha256);

        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long startNano = System.nanoTime();
        long totalSent = 0L;

        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(fileOnDisk), CHUNK_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1 && running.get()) {
                dos.write(buffer, 0, bytesRead);
                BandwidthLimiter.getInstance().throttle(bytesRead);
                totalSent += bytesRead;
                double speed = TransferUtil.calculateSpeedMBps(totalSent, startNano);
                reportSpeedSample(speed);
            }
        } catch (IOException e) {
            socket.close(); // [B8 Fix]: Đóng socket ngay lập tức khi gặp lỗi để client không nhận thiếu byte âm thầm
            throw e;
        }
        dos.flush();

        // Kiểm tra kích thước sau khi gửi để phòng tệp bị sửa đổi giữa chừng (B8)
        if (fileOnDisk.length() != initialSize) {
            socket.close();
            throw new IOException("Tệp bị sửa đổi trong lúc đang truyền!");
        }

        long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
        double avgSpeed = TransferUtil.calculateSpeedMBps(totalSent, startNano);
        reportSpeedSample(0.0);
        logServerEvent("Đã phục vụ tệp \"" + fileOnDisk.getName() + "\" cho Peer " + requesterIp);
        TransferUtil.recordHistory(historyCallback, fileOnDisk.getName(), initialSize, requesterIp, "Gửi (Serve Pull)", avgSpeed, durationMs, "Hợp lệ");
    }

    void handleDownloadChunkCommand(DataInputStream dis, DataOutputStream dos, String requesterIp)
            throws IOException {
        String requestedFileName = dis.readUTF();
        long startOffset = dis.readLong();
        long requestedLength = dis.readLong();

        if (!authenticateClient(dis, dos, requesterIp, CMD_DOWNLOAD_CHUNK)) {
            dos.writeLong(CODE_AUTH_FAILED);
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
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

        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long startNano = System.nanoTime();
        long sentBytes = 0L;

        try (RandomAccessFile raf = new RandomAccessFile(fileOnDisk, "r")) {
            raf.seek(startOffset);

            long remaining = actualLength;
            while (remaining > 0 && running.get()) {
                int maxToRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = raf.read(buffer, 0, maxToRead);
                if (bytesRead == -1) {
                    break;
                }
                dos.write(buffer, 0, bytesRead);
                BandwidthLimiter.getInstance().throttle(bytesRead);
                remaining -= bytesRead;
                sentBytes += bytesRead;

                double speed = TransferUtil.calculateSpeedMBps(sentBytes, startNano);
                reportSpeedSample(speed);
            }
        }
        dos.flush();

        long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
        double avgSpeed = TransferUtil.calculateSpeedMBps(sentBytes, startNano);
        reportSpeedSample(0.0);
        if (startOffset + actualLength >= totalFileSize) {
            TransferUtil.recordHistory(historyCallback, fileOnDisk.getName(), actualLength, requesterIp, "Gửi (Serve Chunk)", avgSpeed, durationMs, "Hợp lệ");
        }
    }

    private void handlePushFileCommand(DataInputStream dis, DataOutputStream dos, String senderIp)
            throws IOException {
        String rawFileName = dis.readUTF();
        long totalBytes = dis.readLong();
        String expectedSha256 = dis.readUTF();

        if (!authenticateClient(dis, dos, senderIp, CMD_PUSH_FILE)) {
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            return;
        }

        // [S5 Fix]: Kiểm tra dung lượng ổ cứng còn trống
        try {
            long usableSpace = Files.getFileStore(downloadDirectory).getUsableSpace();
            if (totalBytes > usableSpace) {
                dos.writeUTF(STATUS_INSUFFICIENT_SPACE);
                dos.flush();
                logServerEvent("Từ chối nhận PUSH_FILE: Ổ cứng không đủ chỗ trống ("
                        + SharedFile.formatSize(totalBytes) + " > " + SharedFile.formatSize(usableSpace) + ")");
                return;
            }
        } catch (IOException ignored) {
        }

        // [S6 Fix]: Cảnh báo nếu tệp là mã thực thi
        String ext = FileContentExtractor.getExtension(rawFileName);
        boolean isExec = EXECUTABLE_EXTENSIONS.contains(ext);
        if (askBeforeAcceptingPush && pushApprovalHandler != null) {
            String prompt = senderIp + " -> " + rawFileName + (isExec ? " [CẢNH BÁO: TỆP THỰC THI NGUY HIỂM!]" : "");
            Boolean approved = pushApprovalHandler.apply(prompt, totalBytes);
            if (!Boolean.TRUE.equals(approved)) {
                dos.writeUTF(STATUS_REJECTED);
                dos.flush();
                logServerEvent("Người dùng đã TỪ CHỐI nhận tệp Push \"" + rawFileName + "\" từ Peer " + senderIp);
                return;
            }
        }

        dos.writeUTF(STATUS_OK);
        dos.flush();
        receivePushedStream(rawFileName, totalBytes, expectedSha256, dis, dos, senderIp);
    }

    private void handleChatMessageCommand(DataInputStream dis, DataOutputStream dos, String senderIp)
            throws IOException {
        String senderName = dis.readUTF();

        if (!authenticateClient(dis, dos, senderIp, CMD_CHAT_MESSAGE)) {
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            return;
        }

        String encryptedBase64 = dis.readUTF();
        String decryptedText;
        try {
            // [S2 Fix]: Decrypt bằng khóa dẫn xuất từ PIN của máy mình (PIN không gửi qua mạng)
            decryptedText = CryptoUtil.decryptTextFromBase64(encryptedBase64, securityPinCode);
        } catch (GeneralSecurityException e) {
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            logServerEvent("Không thể giải mã tin nhắn từ " + senderIp + " (Sai khóa bảo mật)!");
            return;
        }

        dos.writeUTF(STATUS_OK);
        dos.flush();

        String senderDisplay = senderName + " (" + senderIp + ")";
        logServerEvent("[CHAT AES-256] Nhận tin nhắn từ " + senderDisplay + ": " + decryptedText);
        BiConsumer<String, String> cb = this.chatMessageCallback;
        if (cb != null) {
            cb.accept(senderDisplay, decryptedText);
        }
    }

    /**
     * [B5 Fix & B6 Fix]: Nhận luồng Push qua file .part an toàn, xong mới rename và trả xác nhận về sender.
     */
    private void receivePushedStream(
            String rawFileName,
            long totalBytes,
            String expectedSha256,
            DataInputStream dis,
            DataOutputStream dos,
            String senderIp
    ) throws IOException {
        if (totalBytes < 0) {
            throw new IOException("Kích thước tệp không hợp lệ: " + totalBytes);
        }

        String safeFileName = PathUtil.sanitizeFileName(rawFileName);
        Path partPath = downloadDirectory.resolve(safeFileName + ".part");
        File partFile = partPath.toFile();

        reportStatus("Đang nhận \"" + safeFileName + "\" từ " + senderIp + "...");
        logServerEvent("Đang nhận tệp Push \"" + safeFileName + "\" từ Peer " + senderIp);

        MessageDigest digest = HashUtil.newSha256Digest();
        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long totalBytesRead = 0;
        long startNano = System.nanoTime();
        boolean successfullySaved = false;

        try {
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(partFile), CHUNK_BUFFER_SIZE)) {

                long remaining = totalBytes;
                while (remaining > 0 && running.get()) {
                    int maxToRead = (int) Math.min(buffer.length, remaining);
                    int bytesRead = dis.read(buffer, 0, maxToRead);
                    if (bytesRead == -1) {
                        break;
                    }
                    bos.write(buffer, 0, bytesRead);
                    digest.update(buffer, 0, bytesRead);
                    BandwidthLimiter.getInstance().throttle(bytesRead);

                    totalBytesRead += bytesRead;
                    remaining -= bytesRead;

                    double speedMbps = TransferUtil.calculateSpeedMBps(totalBytesRead, startNano);
                    reportSpeedSample(speedMbps);
                }
                bos.flush();
            }

            long durationMs = Math.max(1L, (System.nanoTime() - startNano) / 1_000_000L);
            double avgSpeed = TransferUtil.calculateSpeedMBps(totalBytesRead, startNano);
            reportSpeedSample(0.0);

            if (totalBytesRead != totalBytes) {
                dos.writeUTF("INCOMPLETE");
                dos.flush();
                TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs, "Lỗi (Ngắt kết nối)");
                throw new EOFException("Tệp nhận chưa hoàn chỉnh (" + totalBytesRead + "/" + totalBytes + " bytes)");
            }

            String actualSha256 = HashUtil.toHexString(digest.digest());
            if (!expectedSha256.isBlank() && !expectedSha256.equalsIgnoreCase(actualSha256)) {
                dos.writeUTF(STATUS_HASH_MISMATCH); // [B6 Fix]: Báo lại cho người gửi biết sai lệch SHA
                dos.flush();
                reportStatus("Cảnh báo: File bị hỏng (Sai lệch SHA-256)!");
                logServerEvent("CẢNH BÁO: Tệp Push \"" + safeFileName + "\" sai lệch SHA-256 -> Đã xóa!");
                TransferUtil.recordHistory(historyCallback, safeFileName, totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs, "Lỗi SHA-256");
                return;
            }

            // Chuyển sang tên tệp cuối cùng an toàn
            File finalFile = PathUtil.resolveSafeOutputFile(downloadDirectory, safeFileName);
            Files.move(partPath, finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            successfullySaved = true;

            // [B6 Fix]: Trả mã OK về cho người gửi biết đã nhận trọn vẹn và hợp lệ
            dos.writeUTF(STATUS_OK);
            dos.flush();

            reportStatus("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");
            logServerEvent("Đã nhận hoàn tất \"" + finalFile.getName()
                    + "\" | SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");
            TransferUtil.recordHistory(historyCallback, finalFile.getName(), totalBytes, senderIp, "Nhận (Push)", avgSpeed, durationMs,
                    "Hợp lệ [" + HashUtil.shortHash(actualSha256) + "]");

            if (onFileReceivedCallback != null) {
                onFileReceivedCallback.accept(new SharedFile(
                        finalFile.getName(),
                        finalFile.length(),
                        actualSha256,
                        senderIp,
                        senderIp
                ));
            }
        } finally {
            if (!successfullySaved) {
                try {
                    Files.deleteIfExists(partPath);
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * [S1 Fix]: Xác thực Challenge-Response HMAC-SHA256 trên Server, kèm khóa IP nếu sai quá 5 lần.
     */
    boolean authenticateClient(DataInputStream dis, DataOutputStream dos, String clientIp, String command)
            throws IOException {
        if (!securityPinEnabled || securityPinCode.isEmpty()) {
            dos.writeBoolean(false); // Không yêu cầu xác thực PIN
            dos.flush();
            return true;
        }

        if (isIpLocked(clientIp)) {
            dos.writeBoolean(true);
            dos.writeUTF("LOCKED");
            dos.flush();
            logServerEvent("CẢNH BÁO BẢO MẬT: Từ chối lệnh " + command + " từ " + clientIp
                    + " (IP đang bị tạm khóa 5 phút do nhập sai PIN quá 5 lần)!");
            return false;
        }

        String nonce = CryptoUtil.generateRandomNonce();
        dos.writeBoolean(true); // Yêu cầu xác thực PIN bằng challenge
        dos.writeUTF(nonce);
        dos.flush();

        String clientHmac = dis.readUTF();
        boolean valid = CryptoUtil.verifyChallengeResponse(securityPinCode, nonce, command, clientHmac);
        if (valid) {
            recordSuccessfulAuth(clientIp);
            dos.writeUTF(STATUS_OK);
            dos.flush();
            return true;
        } else {
            recordFailedAttempt(clientIp);
            dos.writeUTF(STATUS_AUTH_FAILED);
            dos.flush();
            logServerEvent("CẢNH BÁO BẢO MẬT: Xác thực mã PIN thất bại cho lệnh " + command + " từ IP " + clientIp);
            return false;
        }
    }

    boolean isIpLocked(String ip) {
        AttemptInfo info = ipAttempts.get(ip);
        if (info == null) {
            return false;
        }
        if (System.currentTimeMillis() < info.lockedUntil) {
            return true;
        }
        if (info.lockedUntil > 0 && System.currentTimeMillis() >= info.lockedUntil) {
            ipAttempts.remove(ip); // Hết thời gian phạt 5 phút
        }
        return false;
    }

    private void recordFailedAttempt(String ip) {
        AttemptInfo info = ipAttempts.computeIfAbsent(ip, k -> new AttemptInfo());
        info.failedCount++;
        if (info.failedCount >= MAX_FAILED_ATTEMPTS) {
            info.lockedUntil = System.currentTimeMillis() + LOCKOUT_DURATION_MS;
            logServerEvent("[BẢO MẬT BÁO ĐỘNG] IP " + ip + " đã bị KHÓA 5 PHÚT do nhập sai mã PIN 5 lần!");
        }
    }

    private void recordSuccessfulAuth(String ip) {
        ipAttempts.remove(ip);
    }

    private void reportStatus(String msg) {
        if (statusMessageCallback != null) {
            statusMessageCallback.accept("Trạng thái: " + msg);
        }
    }

    private void logServerEvent(String message) {
        if (eventLogger != null) {
            eventLogger.accept(message);
        }
    }

    private void reportSpeedSample(double speedMBps) {
        if (speedSampleCallback != null) {
            speedSampleCallback.accept(speedMBps);
        }
    }

    @Override
    public void close() {
        stopServer();
    }

    public void stopServer() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
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

    // ------------------------------------------------------------------------------------------
    // CÁC HÀM TIỆN ÍCH CLIENT-SIDE GỌI TỚI PEER TỪ XA
    // ------------------------------------------------------------------------------------------

    /**
     * Gửi yêu cầu {@code LIST_FILES} tới Peer từ xa với xác thực Challenge-Response an toàn.
     */
    public static List<SharedFile> requestFileListFromPeer(Peer peer, String authPin) throws IOException {
        Objects.requireNonNull(peer, "peer");
        int port = peer.tcpPort() > 0 ? peer.tcpPort() : DEFAULT_PORT;
        String endpoint = (port == DEFAULT_PORT) ? peer.ipAddress() : (peer.ipAddress() + ":" + port);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(peer.ipAddress(), port), LIST_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);

            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(CMD_LIST_FILES);
                dos.flush();

                if (!performClientAuth(dis, dos, authPin, CMD_LIST_FILES)) {
                    throw new IOException("Peer " + endpoint + " từ chối LIST_FILES do sai mã PIN (AUTH_FAILED)!");
                }

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
                                List.of(endpoint),
                                sf.aiTags(),
                                sf.aiSummary(),
                                sf.sensitiveWarning()
                        ));
                    }
                }
                return files;
            }
        }
    }

    public static List<SharedFile> requestFileListFromPeer(Peer peer) throws IOException {
        return requestFileListFromPeer(peer, "");
    }

    /**
     * Tải nhanh tối đa {@code maxPreviewBytes} đầu tiên để xem trước nội dung tệp.
     */
    public static String requestPreviewSnippetFromPeer(
            String targetEndpoint,
            String fileName,
            String authPin,
            int maxPreviewBytes
    ) throws IOException {
        Peer.HostPort hp = Peer.parseHostPort(targetEndpoint, DEFAULT_PORT);
        String safeFileName = PathUtil.sanitizeFileName(fileName);
        int bytesToRequest = Math.max(1024, Math.min(maxPreviewBytes, 32 * 1024));

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hp.host(), hp.port()), LIST_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(CMD_DOWNLOAD_CHUNK);
                dos.writeUTF(safeFileName);
                dos.writeLong(0L);
                dos.writeLong(bytesToRequest);
                dos.flush();

                if (!performClientAuth(dis, dos, authPin, CMD_DOWNLOAD_CHUNK)) {
                    throw new IOException("Peer " + hp.toEndpointString() + " từ chối xem trước do sai mã PIN (AUTH_FAILED)!");
                }

                long remoteTotalSize = dis.readLong();
                if (remoteTotalSize < 0) {
                    throw new IOException("Tệp \"" + safeFileName + "\" không còn tồn tại trên Peer " + hp.toEndpointString());
                }

                long actualChunkLen = dis.readLong();
                dis.readUTF(); // Header SHA-256

                int lenToRead = (int) Math.min(actualChunkLen, bytesToRequest);
                byte[] previewBuffer = new byte[lenToRead];
                int offset = 0;
                while (offset < lenToRead) {
                    int read = dis.read(previewBuffer, offset, lenToRead - offset);
                    if (read == -1) {
                        break;
                    }
                    offset += read;
                }
                return FileContentExtractor.extractSnippetFromBytes(safeFileName, previewBuffer, offset);
            }
        }
    }

    /**
     * Gửi tin nhắn chat bảo mật tới Peer đích bằng AES-256-GCM.
     */
    public static void sendChatMessageToPeer(
            Peer targetPeer,
            String senderName,
            String plainMessage,
            String authPin
    ) throws Exception {
        Objects.requireNonNull(targetPeer, "targetPeer");
        String pin = (authPin == null) ? "" : authPin.trim();
        String encryptedPayload = CryptoUtil.encryptTextToBase64(plainMessage, pin);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetPeer.ipAddress(), targetPeer.tcpPort()), LIST_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(CMD_CHAT_MESSAGE);
                dos.writeUTF(senderName == null || senderName.isBlank() ? "Peer" : senderName);
                dos.flush();

                if (!performClientAuth(dis, dos, pin, CMD_CHAT_MESSAGE)) {
                    throw new IOException("Peer " + targetPeer.endpoint() + " từ chối tin nhắn do sai mã PIN (AUTH_FAILED)!");
                }

                dos.writeUTF(encryptedPayload);
                dos.flush();

                String response = dis.readUTF();
                if (STATUS_AUTH_FAILED.equals(response)) {
                    throw new IOException("Peer " + targetPeer.endpoint() + " không thể giải mã tin nhắn (Sai mã PIN)!");
                }
            }
        }
    }

    /**
     * Helper phía Client xử lý Challenge-Response khi Server yêu cầu PIN.
     */
    public static boolean performClientAuth(
            DataInputStream dis,
            DataOutputStream dos,
            String pin,
            String command
    ) throws IOException {
        boolean challengeRequired = dis.readBoolean();
        if (!challengeRequired) {
            return true; // Server không bật PIN
        }

        String nonceOrStatus = dis.readUTF();
        if ("LOCKED".equalsIgnoreCase(nonceOrStatus)) {
            throw new IOException("Máy chủ từ chối kết nối: IP của bạn đang bị khóa tạm thời 5 phút do nhập sai PIN quá 5 lần!");
        }

        try {
            String hmac = CryptoUtil.createChallengeResponse(pin, nonceOrStatus, command);
            dos.writeUTF(hmac);
            dos.flush();
        } catch (GeneralSecurityException e) {
            throw new IOException("Lỗi tính toán xác thực HMAC: " + e.getMessage(), e);
        }

        String serverReply = dis.readUTF();
        return STATUS_OK.equals(serverReply);
    }
}
