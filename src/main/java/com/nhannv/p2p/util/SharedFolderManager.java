package com.nhannv.p2p.util;

import com.nhannv.p2p.model.SharedFile;
import javafx.concurrent.Task;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Quản lý thư mục chia sẻ ("Shared Folder") tích hợp <b>Java NIO {@link WatchService}</b>
 * để tự động phát hiện tệp thêm mới / chỉnh sửa / xóa theo thời gian thực (Milestone 5).
 *
 * <h2>Giải thích Cơ chế Java NIO {@code WatchService} cho vấn đáp đồ án (Huỳnh Nhân Danh)</h2>
 * <ol>
 *   <li><b>Tại sao dùng {@link WatchService} thay vì dùng vòng lặp {@code while} quét đĩa liên tục?</b>
 *       Nếu dùng vòng lặp {@code Thread.sleep(1000)} rồi gọi {@code folder.listFiles()} liên tục (Polling),
 *       ứng dụng sẽ gây tốn CPU và đọc ổ cứng không cần thiết. Ngược lại, {@code java.nio.file.WatchService}
 *       đăng ký trực tiếp với nhân hệ điều hành (<i>ReadDirectoryChangesW</i> trên Windows). Luồng nền
 *       sẽ ngủ hoàn toàn (0% CPU) tại lệnh {@link WatchService#take()} cho tới khi hệ điều hành bắn sự kiện
 *       có tệp thay đổi!</li>
 *   <li><b>Ba loại sự kiện được lắng nghe:</b>
 *       <ul>
 *         <li>{@link StandardWatchEventKinds#ENTRY_CREATE}: Người dùng vừa copy/thả thêm tệp mới vào thư mục {@code Shared}.</li>
 *         <li>{@link StandardWatchEventKinds#ENTRY_DELETE}: Người dùng vừa xóa một tệp khỏi thư mục {@code Shared}.</li>
 *         <li>{@link StandardWatchEventKinds#ENTRY_MODIFY}: Nội dung tệp trong thư mục {@code Shared} vừa được cập nhật.</li>
 *       </ul>
 *   </li>
 *   <li><b>Cơ chế chống dội sự kiện (Debounce 350ms) &amp; Băm SHA-256 tự động:</b>
 *       Khi Windows Explorer đang copy một tệp lớn vào thư mục {@code Shared}, hệ điều hành sẽ phát ra
 *       hàng loạt sự kiện {@code ENTRY_MODIFY} liên tiếp. Luồng Watcher chờ một khoảng ngắn (350ms) để
 *       quá trình ghi tệp hoàn tất, sau đó tự động tính lại mã băm SHA-256 và gọi callback cập nhật
 *       bảng {@code TableView} trên giao diện qua {@code Platform.runLater()}.</li>
 *   <li><b>Đóng an toàn ({@link #stopWatcher()}):</b>
 *       Khi người dùng tắt ứng dụng, {@code MainApplication.stop()} gọi {@code stopWatcher()} để đóng
 *       {@code WatchService}, giải phóng handle thư mục của hệ điều hành.</li>
 * </ol>
 */
public class SharedFolderManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SharedFolderManager.class.getName());

    /** Tên thư mục chia sẻ mặc định trong thư mục gốc dự án. */
    public static final String DEFAULT_SHARED_DIR_NAME = "Shared";

    private volatile Path sharedFolderPath;
    private final String localHostName;
    private final List<SharedFile> cachedSharedFiles = new CopyOnWriteArrayList<>();

    /** Callback báo về UI khi WatchService phát hiện thay đổi: (danh sách mới, thông điệp sự kiện). */
    private volatile BiConsumer<List<SharedFile>, String> onFolderChangedCallback;

    private final AtomicBoolean watcherRunning = new AtomicBoolean(false);
    private volatile WatchService watchService;
    private volatile WatchKey currentWatchKey;
    private Thread watcherThread;

    public SharedFolderManager() {
        this.sharedFolderPath = Path.of(DEFAULT_SHARED_DIR_NAME).toAbsolutePath();
        this.localHostName = resolveLocalHostName();
        ensureDirectoryExists(this.sharedFolderPath);
    }

    /**
     * Đăng ký callback và khởi động luồng Daemon {@link WatchService} theo dõi thư mục {@code Shared}.
     */
    public synchronized void startRealTimeWatcher(BiConsumer<List<SharedFile>, String> onFolderChangedCallback) {
        this.onFolderChangedCallback = onFolderChangedCallback;
        if (watcherRunning.get()) {
            return;
        }
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
            registerDirectoryWithWatcher(this.sharedFolderPath);
            watcherRunning.set(true);

            watcherThread = new Thread(this::runWatchLoop, "p2p-nio-folder-watcher");
            watcherThread.setDaemon(true);
            watcherThread.start();
            LOG.info(() -> "Đã khởi động Java NIO WatchService cho thư mục: " + sharedFolderPath);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Không thể khởi tạo WatchService cho thư mục chia sẻ", e);
        }
    }

    /**
     * Vòng lặp nền lắng nghe các sự kiện {@code ENTRY_CREATE}, {@code ENTRY_DELETE}, {@code ENTRY_MODIFY}
     * từ hệ điều hành Windows/Linux/macOS.
     */
    private void runWatchLoop() {
        while (watcherRunning.get()) {
            WatchKey key;
            try {
                // Chặn (block) với 0% CPU cho tới khi có tệp được thêm/sửa/xóa trong thư mục Shared.
                key = watchService.take();
            } catch (InterruptedException | ClosedWatchServiceException e) {
                break;
            }

            List<String> eventDescriptions = new ArrayList<>();
            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }
                Object ctx = event.context();
                String fileName = (ctx != null) ? ctx.toString() : "unknown";
                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    eventDescriptions.add("Thêm mới \"" + fileName + "\"");
                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    eventDescriptions.add("Xóa \"" + fileName + "\"");
                } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    eventDescriptions.add("Cập nhật \"" + fileName + "\"");
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                // Thư mục cũ không còn hợp lệ (hoặc đã chuyển sang thư mục mới).
                continue;
            }

            if (!eventDescriptions.isEmpty() && watcherRunning.get()) {
                // Chờ ngắn 350ms (Debounce) để hệ điều hành ghi xong tệp mới vào đĩa trước khi băm SHA-256.
                try {
                    Thread.sleep(350);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                // Quét hết các sự kiện dồn thêm trong lúc chờ 350ms.
                key.pollEvents();

                List<SharedFile> updatedList = scanAndHashSync();
                BiConsumer<List<SharedFile>, String> callback = this.onFolderChangedCallback;
                if (callback != null) {
                    String summary = "WatchService phát hiện thay đổi trong thư mục Shared ("
                            + String.join(", ", eventDescriptions) + ") -> Đã đồng bộ "
                            + updatedList.size() + " tệp.";
                    callback.accept(updatedList, summary);
                }
            }
        }
    }

    private synchronized void registerDirectoryWithWatcher(Path dir) {
        if (watchService == null) {
            return;
        }
        if (currentWatchKey != null) {
            currentWatchKey.cancel();
        }
        try {
            currentWatchKey = dir.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY
            );
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Không thể đăng ký WatchService cho thư mục: " + dir, e);
        }
    }

    /**
     * Đóng {@link WatchService} và dừng luồng theo dõi an toàn khi thoát ứng dụng.
     */
    public synchronized void stopWatcher() {
        watcherRunning.set(false);
        if (currentWatchKey != null) {
            currentWatchKey.cancel();
            currentWatchKey = null;
        }
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException ignored) {
            }
            watchService = null;
        }
        if (watcherThread != null) {
            watcherThread.interrupt();
            watcherThread = null;
        }
    }

    @Override
    public void close() {
        stopWatcher();
    }

    /** Trả về đường dẫn thư mục chia sẻ hiện tại. */
    public Path getSharedFolderPath() {
        return sharedFolderPath;
    }

    /** Đổi thư mục chia sẻ sang thư mục mới do người dùng chọn và tự động chuyển WatchService sang thư mục mới. */
    public synchronized void setSharedFolder(File directory) {
        Objects.requireNonNull(directory, "directory");
        Path path = directory.toPath().toAbsolutePath();
        ensureDirectoryExists(path);
        this.sharedFolderPath = path;
        registerDirectoryWithWatcher(path);
    }

    /**
     * Sao chép danh sách tệp (do người dùng Kéo &amp; Thả từ ngoài màn hình vào {@code TableView})
     * vào thư mục {@code Shared} hiện tại. {@link WatchService} sẽ tự động bắt sự kiện và băm SHA-256!
     */
    public int copyDroppedFilesToSharedFolder(List<File> droppedFiles) throws IOException {
        if (droppedFiles == null || droppedFiles.isEmpty()) {
            return 0;
        }
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);
        int copied = 0;
        for (File src : droppedFiles) {
            if (src != null && src.exists() && src.isFile()) {
                Path target = folder.resolve(src.getName()).normalize();
                if (!src.toPath().toAbsolutePath().normalize().equals(target)) {
                    Files.copy(src.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
                }
                copied++;
            }
        }
        return copied;
    }

    /** Lấy danh sách tệp chia sẻ đã được băm SHA-256 gần nhất (an toàn đa luồng). */
    public List<SharedFile> getSharedFiles() {
        return Collections.unmodifiableList(new ArrayList<>(cachedSharedFiles));
    }

    /**
     * Tìm tệp thực tế trên đĩa trong thư mục chia sẻ theo tên tệp để phục vụ lệnh {@code DOWNLOAD_FILE} / {@code DOWNLOAD_CHUNK}.
     */
    public File resolveSharedFileOnDisk(String requestedFileName) {
        if (requestedFileName == null || requestedFileName.isBlank()) {
            return null;
        }
        String safeName = Path.of(requestedFileName).getFileName().toString();
        Path resolved = sharedFolderPath.resolve(safeName).normalize();
        if (!resolved.startsWith(sharedFolderPath)) {
            return null;
        }
        File file = resolved.toFile();
        return (file.exists() && file.isFile()) ? file : null;
    }

    /** Tìm metadata {@link SharedFile} đã băm sẵn theo tên tệp. */
    public SharedFile findMetadataByName(String fileName) {
        if (fileName == null) {
            return null;
        }
        for (SharedFile sf : cachedSharedFiles) {
            if (sf.fileName().equals(fileName)) {
                return sf;
            }
        }
        return null;
    }

    /**
     * Quét đồng bộ thư mục chia sẻ và tính SHA-256 cho từng tệp.
     * <b>Chỉ gọi hàm này từ luồng nền</b>.
     */
    public List<SharedFile> scanAndHashSync() {
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);

        File[] files = folder.toFile().listFiles(File::isFile);
        if (files == null || files.length == 0) {
            cachedSharedFiles.clear();
            return List.of();
        }

        List<SharedFile> result = new ArrayList<>(files.length);
        for (File file : files) {
            try {
                String sha256 = HashUtil.calculateSHA256(file);
                result.add(new SharedFile(
                        file.getName(),
                        file.length(),
                        sha256,
                        "LOCAL",
                        localHostName
                ));
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Không thể băm SHA-256 cho tệp: " + file.getName(), e);
            }
        }

        cachedSharedFiles.clear();
        cachedSharedFiles.addAll(result);
        return Collections.unmodifiableList(result);
    }

    /**
     * Tạo một JavaFX {@link Task} quét thư mục chia sẻ và tính SHA-256 trên luồng nền.
     */
    public Task<List<SharedFile>> createScanTask() {
        return new Task<>() {
            @Override
            protected List<SharedFile> call() {
                Path folder = sharedFolderPath;
                ensureDirectoryExists(folder);

                File[] files = folder.toFile().listFiles(File::isFile);
                if (files == null || files.length == 0) {
                    cachedSharedFiles.clear();
                    updateProgress(1.0, 1.0);
                    updateMessage("Trạng thái: Thư mục chia sẻ (" + folder.getFileName() + ") đang trống");
                    return List.of();
                }

                List<SharedFile> scanned = new ArrayList<>(files.length);
                int total = files.length;
                for (int i = 0; i < total; i++) {
                    if (isCancelled()) {
                        break;
                    }
                    File f = files[i];
                    updateProgress(i, total);
                    updateMessage("Trạng thái: Đang tính SHA-256 (" + (i + 1) + "/" + total + "): " + f.getName());
                    try {
                        String sha256 = HashUtil.calculateSHA256(f);
                        scanned.add(new SharedFile(
                                f.getName(),
                                f.length(),
                                sha256,
                                "LOCAL",
                                localHostName
                        ));
                    } catch (IOException e) {
                        LOG.log(Level.WARNING, "Bỏ qua tệp lỗi khi tính SHA-256: " + f.getName(), e);
                    }
                }

                cachedSharedFiles.clear();
                cachedSharedFiles.addAll(scanned);
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã quét xong " + scanned.size()
                        + " tệp trong thư mục chia sẻ (" + folder.getFileName() + ")");
                return Collections.unmodifiableList(scanned);
            }
        };
    }

    private static void ensureDirectoryExists(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Không thể tạo thư mục chia sẻ: " + dir, e);
        }
    }

    private static String resolveLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            String env = System.getenv("COMPUTERNAME");
            return (env != null && !env.isBlank()) ? env : "My-PC";
        }
    }
}
