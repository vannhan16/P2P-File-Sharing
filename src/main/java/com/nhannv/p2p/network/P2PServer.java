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
 * Máy chủ TCP Đa Lệnh (Multi-Command P2P TCP Server) nâng cấp cho Milestone 3.
 *
 * <h2>Giải thích Giao thức TCP Đa Lệnh (Application Command Protocol) cho Huỳnh Nhân Danh</h2>
 * Thay vì mỗi cổng chỉ làm một việc cố định, mọi kết nối TCP tới cổng {@value #DEFAULT_PORT}
 * đều gửi một chuỗi lệnh đầu tiên (<b>Command Header</b> bằng {@link DataOutputStream#writeUTF(String)})
 * để Server phân nhánh xử lý trên {@link ExecutorService} (Thread Pool):
 *
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────────────────────┐
 * │ 1. Lệnh "LIST_FILES" (Truy vấn danh sách tệp chia sẻ của Peer)                   │
 * │    • Client -> Server : writeUTF("LIST_FILES")                                   │
 * │    • Server -> Client : writeInt(fileCount)                                      │
 * │                         Lặp fileCount lần: writeUTF("name|size|sha256|ip|owner") │
 * ├──────────────────────────────────────────────────────────────────────────────────┤
 * │ 2. Lệnh "DOWNLOAD_FILE" (Pull Protocol - Tải tệp từ Shared Folder của Peer)      │
 * │    • Client -> Server : writeUTF("DOWNLOAD_FILE") + writeUTF(requestedFileName)  │
 * │    • Server -> Client :                                                          │
 * │        - Nếu không tìm thấy: writeLong(-1L)                                      │
 * │        - Nếu hợp lệ        : writeLong(fileSize) + writeUTF(sha256Hash)          │
 * │                              + Luồng Raw Bytes cắt theo từng Chunk 64KB          │
 * ├──────────────────────────────────────────────────────────────────────────────────┤
 * │ 3. Lệnh "PUSH_FILE" (Push Protocol - Chủ động đẩy tệp sang máy bạn)              │
 * │    • Client -> Server : writeUTF("PUSH_FILE") + writeUTF(fileName)               │
 * │                         + writeLong(fileSize) + writeUTF(sha256Hash)             │
 * │                         + Luồng Raw Bytes theo từng Chunk 64KB                   │
 * │    • Server nhận theo từng Chunk 64KB, băm SHA-256 On-the-fly và đối chiếu ngay! │
 * └──────────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 */
public class P2PServer extends Task<Void> {

    private static final Logger LOG = Logger.getLogger(P2PServer.class.getName());

    /** Cổng TCP mặc định cho mọi lệnh P2P. */
    public static final int DEFAULT_PORT = 8889;

    /** Các mã lệnh (Command Header) chuẩn của giao thức P2P. */
    public static final String CMD_LIST_FILES = "LIST_FILES";
    public static final String CMD_DOWNLOAD_FILE = "DOWNLOAD_FILE";
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

    /** Thread Pool xử lý đồng thời nhiều Peer cùng truy vấn danh sách hoặc tải tệp. */
    private final ExecutorService clientWorkerPool;
    private volatile ServerSocket serverSocket;

    public P2PServer(SharedFolderManager sharedFolderManager, Consumer<SharedFile> onFileReceivedCallback) {
        this(DEFAULT_PORT, sharedFolderManager, onFileReceivedCallback);
    }

    public P2PServer(int port, SharedFolderManager sharedFolderManager, Consumer<SharedFile> onFileReceivedCallback) {
        this.port = port;
        this.sharedFolderManager = Objects.requireNonNull(sharedFolderManager, "sharedFolderManager");
        this.downloadDirectory = Path.of(DOWNLOADS_DIR_NAME).toAbsolutePath();
        this.onFileReceivedCallback = onFileReceivedCallback;

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
                    // Giao từng kết nối cho Thread Pool xử lý song song, không làm nghẽn vòng lặp accept().
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

            // Đọc Command Header đầu tiên.
            String command = dis.readUTF();

            switch (command) {
                case CMD_LIST_FILES -> handleListFilesCommand(dos);
                case CMD_DOWNLOAD_FILE -> handleDownloadFileCommand(dis, dos, peerIp);
                case CMD_PUSH_FILE -> handlePushFileCommand(dis, peerIp, true);
                default -> {
                    // Tương thích ngược: nếu Peer cũ gửi thẳng tên tệp (chưa có header PUSH_FILE).
                    handleLegacyPushFile(command, dis, peerIp);
                }
            }
        } catch (EOFException e) {
            LOG.log(Level.WARNING, "Peer " + peerIp + " ngắt kết nối đột ngột", e);
            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Cảnh báo - Peer " + peerIp + " ngắt kết nối đột ngột!");
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Lỗi xử lý yêu cầu từ Peer " + peerIp, e);
            updateProgress(0.0, 1.0);
            updateMessage("Trạng thái: Lỗi giao tiếp với Peer " + peerIp + " - " + e.getMessage());
        }
    }

    /**
     * Nhánh 1: Xử lý lệnh {@code "LIST_FILES"}.
     * Trả về danh sách các {@link SharedFile} đang có trong Shared Folder của máy mình.
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
     * Nhánh 2: Xử lý lệnh {@code "DOWNLOAD_FILE"}.
     * Đọc tên tệp yêu cầu -> gửi kích thước tệp + mã SHA-256 gốc -> đẩy dữ liệu theo từng chunk 64KB.
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

        // Gửi Header phản hồi: kích thước tệp + mã SHA-256 gốc.
        dos.writeLong(fileSize);
        dos.writeUTF(sha256);

        // Đẩy luồng dữ liệu tệp theo từng chunk 64KB.
        byte[] buffer = new byte[CHUNK_BUFFER_SIZE];
        long totalSent = 0;
        long startNano = System.nanoTime();

        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(fileOnDisk), CHUNK_BUFFER_SIZE)) {
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1 && !isCancelled()) {
                dos.write(buffer, 0, bytesRead);
                totalSent += bytesRead;

                double speed = calculateSpeedMBps(totalSent, startNano);
                updateMessage(String.format(Locale.US,
                        "Trạng thái: Đang gửi \"%s\" cho %s (%.2f MB/s)",
                        fileOnDisk.getName(), requesterIp, speed));
            }
        }
        dos.flush();
        updateMessage("Trạng thái: Đã phục vụ xong tệp \"" + fileOnDisk.getName() + "\" cho " + requesterIp);
    }

    /**
     * Nhánh 3: Xử lý lệnh {@code "PUSH_FILE"}.
     * Nhận tệp do một Peer chủ động đẩy sang, băm SHA-256 On-the-fly ngay khi đọc các chunk 64KB
     * và đối chiếu với mã SHA-256 gốc trong Header.
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

        // Khởi tạo MessageDigest để băm SHA-256 On-the-fly ngay trong lúc đọc luồng mạng.
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
                // ON-THE-FLY HASHING: Cập nhật từng khối byte vừa nhận vào bộ băm SHA-256.
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
            return;
        }

        updateProgress(1.0, 1.0);
        updateMessage("Tải hoàn tất! SHA-256 hợp lệ: [" + HashUtil.shortHash(actualSha256) + "]");

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
     * Hàm tiện ích phía Client: gửi lệnh {@code "LIST_FILES"} tới một {@link Peer}
     * và nhận về danh sách {@link SharedFile} mà Peer đó đang chia sẻ.
     * <b>Luôn gọi hàm này từ Background Thread!</b>
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
                        // Đảm bảo thông tin tên máy/IP của Peer hiển thị chính xác.
                        files.add(new SharedFile(
                                sf.fileName(),
                                sf.fileSize(),
                                sf.sha256Hash(),
                                peer.ipAddress(),
                                peer.hostName()
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
