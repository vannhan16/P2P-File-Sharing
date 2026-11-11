package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Máy chủ TCP Đa Lệnh (Multi-Command P2P TCP Server) hoàn chỉnh cho Milestone 4.
 *
 * <h2>Giải thích Giao thức TCP Đa Lệnh & RandomAccessFile.seek() cho vấn đáp đồ án</h2>
 * Mọi kết nối TCP tới cổng {@value #DEFAULT_PORT} đều gửi chuỗi lệnh đầu tiên (<b>Command Header</b>)
 * để Server phân nhánh xử lý song song trên {@link ExecutorService}:
 * <ol>
 *   <li><b>{@code "LIST_FILES"}:</b> Trả về danh sách {@link SharedFile} trong Shared Folder.</li>
 *   <li><b>{@code "DOWNLOAD_FILE"}:</b> Tải nguyên tệp từ byte 0 đến hết tệp.</li>
 *   <li><b>{@code "DOWNLOAD_CHUNK"} (MỚI - Milestone 4):</b>
 *       <ul>
 *         <li>Client gửi: {@code fileName} (UTF) + {@code startOffset} (long) + {@code length} (long).</li>
 *         <li>Server mở tệp bằng {@link RandomAccessFile} ở chế độ {@code "r"} (chỉ đọc) và gọi
 *             {@link RandomAccessFile#seek(long) raf.seek(startOffset)}.</li>
 *         <li><b>Tại sao dùng {@code RandomAccessFile.seek(startOffset)}?</b>
 *             Với {@code FileInputStream} thông thường, muốn nhảy tới byte thứ 4GB thì phải đọc bỏ qua
 *             tuần tự 4GB đầu tiên trên đĩa. Ngược lại, {@code RandomAccessFile.seek(startOffset)}
 *             di chuyển con trỏ tệp (File Pointer) của hệ điều hành nhảy thẳng tới đúng vị trí byte
 *             {@code startOffset} với độ phức tạp <b>O(1) (tức thì, 0ms)</b>, sau đó chỉ đọc và truyền
 *             đúng {@code length} byte được yêu cầu.</li>
 *         <li>Nhờ lệnh {@code DOWNLOAD_CHUNK} duy nhất này, hệ thống giải quyết được đồng thời cả
 *             <b>Pause/Resume</b> (tải tiếp từ byte bị đứt) lẫn <b>Multi-source Swarm Download</b>
 *             (nhiều luồng xin các đoạn {@code [startOffset, length]} khác nhau từ nhiều Peer cùng lúc).</li>
 *       </ul>
 *   </li>
 *   <li><b>{@code "PUSH_FILE"}:</b> Nhận tệp đẩy trực tiếp và xác thực SHA-256 On-the-fly.</li>
 * </ol>
 */
public class P2PServer extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(P2PServer.class.getName());

    /** Cổng TCP mặc định cho mọi lệnh P2P. */
    public static final int DEFAULT_PORT = 8889;

    /** Các mã lệnh (Command Header) chuẩn của giao thức P2P. */
    public static final String CMD_PING = "PING";
    public static final String CMD_LIST_FILES = "LIST_FILES";
    public static final String CMD_DOWNLOAD_FILE = "DOWNLOAD_FILE";
    public static final String CMD_DOWNLOAD_CHUNK = "DOWNLOAD_CHUNK";
    public static final String CMD_PUSH_FILE = "PUSH_FILE";

    /** Kích thước mỗi khối (Chunk size) đọc/ghi mạng và đĩa = 64 KB. */
    public static final int CHUNK_BUFFER_SIZE = 64 * 1024;

    /** Thư mục mặc định lưu các tệp tải về hoặc được Push tới. */
    public static final String DOWNLOADS_DIR_NAME = "Downloads";

    private static final int SOCKET_TIMEOUT_MS = 15_000;
    private static final int LIST_CONNECT_TIMEOUT_MS = 2_500;

    private final int port;
    private final SharedFolderManager sharedFolderManager;
    private final Path downloadDirectory;
    private final Consumer<SharedFile> onFileReceivedCallback;
    private final Consumer<String> eventLogger;
    private DiscoveryService discoveryService;

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

    /**
     * Đọc dòng lệnh đầu tiên ({@code Command Header}) từ Client và phân nhánh xử lý.
     */
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
                case CMD_PUSH_FILE -> handlePushFileCommand(dis, peerIp, true);
                default -> handleLegacyPushFile(command, dis, peerIp);
            }
        } catch (EOFException e) {
            LOG.log(Level.FINE, "Peer " + peerIp + " đã đóng luồng kết nối", e);
        } catch (IOException e) {
            LOG.log(Level.FINE, "Kết thúc phiên giao tiếp với Peer " + peerIp + ": " + e.getMessage());
        }
    }

    /** Gắn DiscoveryService để khi nhận lệnh PING qua TCP cũng tự động ghi nhận Peer gửi. */
    public void setDiscoveryService(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    /**
     * Phản hồi lệnh {@code "PING"} phục vụ tính năng TCP Active Subnet Scan:
     * Đọc tên máy của bên quét (để cả 2 chiều đều thấy nhau tức thì) và trả về tên máy của mình.
     */
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
        dos.flush();
    }

    /**
     * Nhánh 1: Xử lý lệnh {@code "LIST_FILES"}.
     */
    private void handleListFilesCommand(DataOutputStream dos) throws IOException {
        List<SharedFile> files = sharedFolderManager.getSharedFiles();
        dos.writeInt(files.size());
        for (SharedFile sf : files) {
            dos.writeUTF(sf.toWireLine());
        }
        dos.flush();
    }

    /**
     * Nhánh 2: Xử lý lệnh {@code "DOWNLOAD_FILE"} (tải toàn bộ tệp từ đầu).
     */
    private void handleDownloadFileCommand(DataInputStream dis, DataOutputStream dos, String requesterIp)
            throws IOException {
        String requestedFileName = dis.readUTF();
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
        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(fileOnDisk), CHUNK_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1 && !isCancelled()) {
                dos.write(buffer, 0, bytesRead);
            }
        }
        dos.flush();
        logServerEvent("Đã gửi toàn bộ tệp \"" + fileOnDisk.getName() + "\" tới Peer " + requesterIp);
    }

    /**
     * Nhánh 3 (MỚI - Milestone 4): Xử lý lệnh {@code "DOWNLOAD_CHUNK"}.
     * <p>
     * <b>Cơ chế hoạt động:</b>
     * <ol>
     *   <li>Đọc 3 tham số từ Client: {@code fileName}, {@code startOffset} (vị trí byte bắt đầu),
     *       và {@code length} (số lượng byte cần lấy).</li>
     *   <li>Gửi phản hồi Header gồm: {@code totalFileSize} (để Client kiểm tra kích thước tổng),
     *       {@code actualChunkLength} (số byte thực tế sẽ gửi), và {@code sha256Hash} gốc của tệp.</li>
     *   <li>Mở tệp bằng {@code new RandomAccessFile(fileOnDisk, "r")} (chế độ chỉ đọc, an toàn cho
     *       nhiều luồng cùng đọc song song), gọi {@code raf.seek(startOffset)} để nhảy thẳng tới
     *       vị trí byte bắt đầu, rồi đọc từng khối 64KB cho tới khi gửi đủ {@code actualChunkLength} byte.</li>
     * </ol>
     */
    private void handleDownloadChunkCommand(DataInputStream dis, DataOutputStream dos, String requesterIp)
            throws IOException {
        String requestedFileName = dis.readUTF();
        long startOffset = dis.readLong();
        long requestedLength = dis.readLong();

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

        // Số byte thực tế có thể đọc từ vị trí startOffset (không vượt quá phần còn lại của tệp).
        long actualLength = Math.min(requestedLength, totalFileSize - startOffset);

        SharedFile meta = sharedFolderManager.findMetadataByName(fileOnDisk.getName());
        String sha256 = (meta != null) ? meta.sha256Hash() : HashUtil.calculateSHA256(fileOnDisk);

        // Gửi Header phản hồi cho DOWNLOAD_CHUNK:
        // [totalFileSize (long)] + [actualLength (long)] + [sha256 (UTF)]
        dos.writeLong(totalFileSize);
        dos.writeLong(actualLength);
        dos.writeUTF(sha256);

        logServerEvent(String.format(Locale.US,
                "Phục vụ DOWNLOAD_CHUNK \"%s\" cho %s [offset=%d, length=%d bytes]",
                fileOnDisk.getName(), requesterIp, startOffset, actualLength));

        // Mở tệp bằng RandomAccessFile ở chế độ "r" (read-only) và nhảy thẳng tới startOffset.
        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        try (RandomAccessFile raf = new RandomAccessFile(fileOnDisk, "r")) {
            // Nhảy con trỏ tệp thẳng tới vị trí byte bắt đầu (độ phức tạp O(1)).
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
            }
        }
        dos.flush();
    }

    /**
     * Nhánh 4: Xử lý lệnh {@code "PUSH_FILE"}.
     */
    private void handlePushFileCommand(DataInputStream dis, String senderIp, boolean hasSha256Header)
            throws IOException {
        String rawFileName = dis.readUTF();
        long totalBytes = dis.readLong();
        String expectedSha256 = hasSha256Header ? dis.readUTF() : "";
        receivePushedStream(rawFileName, totalBytes, expectedSha256, dis, senderIp);
    }

    private void handleLegacyPushFile(String rawFileName, DataInputStream dis, String senderIp)
            throws IOException {
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
                updateMessage(String.format(Locale.US,
                        "Trạng thái: Đang nhận \"%s\" từ %s (%.2f MB/s)",
                        outputFile.getName(), senderIp, speedMbps));
            }
            bos.flush();
        }

        if (totalBytesRead != totalBytes) {
            Files.deleteIfExists(outputFile.toPath());
            throw new EOFException("Tệp nhận chưa hoàn chỉnh (" + totalBytesRead + "/" + totalBytes + " bytes)");
        }

        String actualSha256 = HashUtil.toHexString(digest.digest());
        if (!expectedSha256.isBlank() && !expectedSha256.equalsIgnoreCase(actualSha256)) {
            Files.deleteIfExists(outputFile.toPath());
            updateProgress(0.0, 1.0);
            updateMessage("Cảnh báo: File bị hỏng trong quá trình truyền (Sai lệch SHA-256)!");
            logServerEvent("CẢNH BÁO: Tệp Push \"" + outputFile.getName() + "\" sai lệch SHA-256 -> Đã xóa!");
            return;
        }

        updateProgress(1.0, 1.0);
        updateMessage("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");
        logServerEvent("Đã nhận hoàn tất \"" + outputFile.getName()
                + "\" | SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");

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
     * Hàm tiện ích phía Client: gửi lệnh {@code "LIST_FILES"} tới một {@link Peer}.
     */
    public static List<SharedFile> requestFileListFromPeer(Peer peer) throws IOException {
        return requestFileListFromPeer(peer, DEFAULT_PORT);
    }

    public static List<SharedFile> requestFileListFromPeer(Peer peer, int port) throws IOException {
        Objects.requireNonNull(peer, "peer");
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
                    SharedFile sf = SharedFile.fromWireLine(line, peer.ipAddress());
                    if (sf != null) {
                        files.add(new SharedFile(
                                sf.fileName(),
                                sf.fileSize(),
                                sf.sha256Hash(),
                                peer.ipAddress(),
                                peer.hostName(),
                                List.of(peer.ipAddress())
                        ));
                    }
                }
                return files;
            }
        }
    }

    /** Tránh ghi đè tệp cũ nếu trùng tên trong thư mục Downloads. */
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

    private static double calculateSpeedMBps(long bytesTransferred, long startTimeNano) {
        double elapsedSeconds = (System.nanoTime() - startTimeNano) / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0001) {
            return 0.0;
        }
        return (bytesTransferred / (1024.0 * 1024.0)) / elapsedSeconds;
    }

    /** Đóng ServerSocket và giải phóng Thread Pool an toàn khi tắt ứng dụng. */
    public void stopServer() {
        cancel(true);
        ServerSocket ss = this.serverSocket;
        if (ss != null && !ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException ignored) {
                // Bỏ qua khi đóng.
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
