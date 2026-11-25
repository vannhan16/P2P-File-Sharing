package com.nhannv.p2p.util;

import com.nhannv.p2p.ai.AIManager;
import com.nhannv.p2p.ai.AIMetadata;
import com.nhannv.p2p.model.SharedFile;
import javafx.concurrent.Task;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Quản lý thư mục chia sẻ ("Shared Folder") tích hợp <b>Java NIO {@link WatchService}</b>
 * để tự động phát hiện tệp thêm mới / chỉnh sửa / xóa theo thời gian thực (Milestone 5).
 *
 * <h2>Các Cải tiến Độ tin cậy &amp; Toàn vẹn (B7)</h2>
 * <ul>
 *   <li><b>Cache Freshness theo Size &amp; LastModified:</b> Lưu trữ bộ ba {@code (size, lastModified, sha256)}.
 *       Khi phục vụ tệp, nếu kích thước hoặc thời điểm sửa đổi trên đĩa khác với cache, hệ thống tự động băm lại.</li>
 *   <li><b>Chống băm tệp đang copy dở (Stability Polling):</b> Luồng Watcher kiểm tra kích thước tệp ổn định
 *       (không tăng thêm trong 1 giây) trước khi tiến hành tính toán mã băm SHA-256.</li>
 * </ul>
 */
public class SharedFolderManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SharedFolderManager.class.getName());

    public static final String DEFAULT_SHARED_DIR_NAME = "Shared";

    public record FileHashEntry(long size, long lastModified, String sha256, AIMetadata aiMetadata) {
    }

    private volatile Path sharedFolderPath;
    private final String localHostName;
    private final List<SharedFile> cachedSharedFiles = new CopyOnWriteArrayList<>();
    private final Map<String, FileHashEntry> fileStatsCache = new ConcurrentHashMap<>();

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
     * Vòng lặp nền lắng nghe sự kiện từ WatchService kèm cơ chế chờ file ổn định (B7).
     */
    private void runWatchLoop() {
        while (watcherRunning.get()) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException | ClosedWatchServiceException e) {
                break;
            }

            List<String> eventDescriptions = new ArrayList<>();
            List<String> modifiedOrCreatedFiles = new ArrayList<>();

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }
                Object ctx = event.context();
                String fileName = (ctx != null) ? ctx.toString() : "unknown";
                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    eventDescriptions.add("Thêm mới \"" + fileName + "\"");
                    modifiedOrCreatedFiles.add(fileName);
                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    eventDescriptions.add("Xóa \"" + fileName + "\"");
                    fileStatsCache.remove(fileName);
                } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    eventDescriptions.add("Cập nhật \"" + fileName + "\"");
                    modifiedOrCreatedFiles.add(fileName);
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                continue;
            }

            if (!eventDescriptions.isEmpty() && watcherRunning.get()) {
                // [B7 Fix]: Chờ tệp ổn định (không thay đổi kích thước trong 1 giây) trước khi băm
                waitForFilesStabilized(modifiedOrCreatedFiles);

                // Lấy hết các sự kiện dồn thêm
                key.pollEvents();

                List<SharedFile> updatedList = scanAndHashSync();
                BiConsumer<List<SharedFile>, String> callback = this.onFolderChangedCallback;
                if (callback != null) {
                    String summary = "WatchService phát hiện thay đổi ("
                            + String.join(", ", eventDescriptions) + ") -> Đã đồng bộ "
                            + updatedList.size() + " tệp.";
                    callback.accept(updatedList, summary);
                }
            }
        }
    }

    /**
     * Chờ cho đến khi tất cả các tệp đang ghi dở ngừng thay đổi kích thước.
     */
    private void waitForFilesStabilized(List<String> fileNames) {
        if (fileNames.isEmpty()) {
            return;
        }
        try {
            Thread.sleep(600);
            for (String name : fileNames) {
                File f = sharedFolderPath.resolve(name).toFile();
                if (!f.exists() || !f.isFile()) {
                    continue;
                }
                long len1 = f.length();
                Thread.sleep(400);
                long len2 = f.length();
                if (len1 != len2) {
                    // File vẫn đang được copy, chờ thêm 1 giây
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    public Path getSharedFolderPath() {
        return sharedFolderPath;
    }

    public synchronized void setSharedFolder(File directory) {
        Objects.requireNonNull(directory, "directory");
        Path path = directory.toPath().toAbsolutePath();
        ensureDirectoryExists(path);
        this.sharedFolderPath = path;
        fileStatsCache.clear();
        registerDirectoryWithWatcher(path);
    }

    public int copyDroppedFilesToSharedFolder(List<File> droppedFiles) throws IOException {
        if (droppedFiles == null || droppedFiles.isEmpty()) {
            return 0;
        }
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);
        int copied = 0;
        for (File src : droppedFiles) {
            if (src == null || !src.exists()) {
                continue;
            }
            if (src.isDirectory()) {
                packageDirectoryToSharedZip(src);
                copied++;
            } else if (src.isFile()) {
                Path target = folder.resolve(src.getName()).normalize();
                if (!src.toPath().toAbsolutePath().normalize().equals(target)) {
                    Files.copy(src.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
                }
                copied++;
            }
        }
        return copied;
    }

    public File packageDirectoryToSharedZip(File sourceDirectory) throws IOException {
        Objects.requireNonNull(sourceDirectory, "sourceDirectory");
        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            throw new IOException("Thư mục nguồn không hợp lệ: " + sourceDirectory);
        }
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);
        File targetZip = folder.resolve(sourceDirectory.getName() + ".zip").toFile();
        zipDirectoryToFile(sourceDirectory, targetZip);
        return targetZip;
    }

    public static void zipDirectoryToFile(File sourceDir, File destinationZip) throws IOException {
        Path rootPath = sourceDir.toPath().toAbsolutePath().normalize();
        Path destPath = destinationZip.toPath().toAbsolutePath().normalize();
        byte[] buffer = new byte[HashUtil.HASH_BUFFER_SIZE];

        try (ZipOutputStream zos = new ZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(destinationZip), HashUtil.HASH_BUFFER_SIZE))) {
            try (var walkStream = Files.walk(rootPath)) {
                for (Path currentPath : (Iterable<Path>) walkStream::iterator) {
                    if (Files.isDirectory(currentPath) || currentPath.equals(destPath)) {
                        continue;
                    }
                    String relativeEntryName = sourceDir.getName() + "/"
                            + rootPath.relativize(currentPath).toString().replace('\\', '/');
                    zos.putNextEntry(new ZipEntry(relativeEntryName));
                    try (BufferedInputStream bis = new BufferedInputStream(
                            new FileInputStream(currentPath.toFile()), HashUtil.HASH_BUFFER_SIZE)) {
                        int read;
                        while ((read = bis.read(buffer)) != -1) {
                            zos.write(buffer, 0, read);
                        }
                    }
                    zos.closeEntry();
                }
            }
            zos.flush();
        }
    }

    public List<SharedFile> getSharedFiles() {
        return Collections.unmodifiableList(new ArrayList<>(cachedSharedFiles));
    }

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

    /**
     * [B7 Fix]: Tìm metadata kèm xác thực tính tươi mới (Freshness check).
     * Nếu file trên đĩa đã đổi kích thước hoặc lastModified so với cache, băm lại ngay!
     */
    public SharedFile findMetadataByName(String fileName) {
        if (fileName == null) {
            return null;
        }
        File diskFile = resolveSharedFileOnDisk(fileName);
        if (diskFile == null || !diskFile.exists()) {
            return null;
        }

        FileHashEntry entry = fileStatsCache.get(fileName);
        if (entry != null && entry.size() == diskFile.length() && entry.lastModified() == diskFile.lastModified()) {
            return new SharedFile(
                    fileName,
                    entry.size(),
                    entry.sha256(),
                    "LOCAL",
                    localHostName
            ).withAiMetadata(entry.aiMetadata());
        }

        // Đã đổi nội dung: băm lại đồng bộ
        try {
            String freshHash = HashUtil.calculateSHA256(diskFile);
            AIMetadata aiMeta = AIManager.getInstance().analyzeFileWithCache(diskFile, freshHash);
            fileStatsCache.put(fileName, new FileHashEntry(diskFile.length(), diskFile.lastModified(), freshHash, aiMeta));
            return new SharedFile(
                    fileName,
                    diskFile.length(),
                    freshHash,
                    "LOCAL",
                    localHostName
            ).withAiMetadata(aiMeta);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Không thể cập nhật hash cho file: " + fileName, e);
            return null;
        }
    }

    public List<SharedFile> scanAndHashSync() {
        Path folder = this.sharedFolderPath;
        ensureDirectoryExists(folder);

        File[] files = folder.toFile().listFiles(File::isFile);
        if (files == null || files.length == 0) {
            cachedSharedFiles.clear();
            fileStatsCache.clear();
            return List.of();
        }

        List<SharedFile> result = new ArrayList<>(files.length);
        for (File file : files) {
            try {
                String fileName = file.getName();
                long curSize = file.length();
                long curMod = file.lastModified();

                FileHashEntry cached = fileStatsCache.get(fileName);
                String sha256;
                AIMetadata aiMeta;

                if (cached != null && cached.size() == curSize && cached.lastModified() == curMod) {
                    sha256 = cached.sha256();
                    aiMeta = cached.aiMetadata();
                } else {
                    sha256 = HashUtil.calculateSHA256(file);
                    aiMeta = AIManager.getInstance().analyzeFileWithCache(file, sha256);
                    fileStatsCache.put(fileName, new FileHashEntry(curSize, curMod, sha256, aiMeta));
                }

                SharedFile sharedFile = new SharedFile(
                        fileName,
                        curSize,
                        sha256,
                        "LOCAL",
                        localHostName
                ).withAiMetadata(aiMeta);
                result.add(sharedFile);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Không thể băm SHA-256 cho tệp: " + file.getName(), e);
            }
        }

        cachedSharedFiles.clear();
        cachedSharedFiles.addAll(result);
        return Collections.unmodifiableList(result);
    }

    public Task<List<SharedFile>> createScanTask() {
        return new Task<>() {
            @Override
            protected List<SharedFile> call() {
                Path folder = sharedFolderPath;
                ensureDirectoryExists(folder);

                File[] files = folder.toFile().listFiles(File::isFile);
                if (files == null || files.length == 0) {
                    cachedSharedFiles.clear();
                    fileStatsCache.clear();
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
                    updateMessage("Trạng thái: Đang băm SHA-256 & gắn thẻ AI (" + (i + 1) + "/" + total + "): " + f.getName());
                    try {
                        String fileName = f.getName();
                        long curSize = f.length();
                        long curMod = f.lastModified();

                        FileHashEntry cached = fileStatsCache.get(fileName);
                        String sha256;
                        AIMetadata aiMeta;

                        if (cached != null && cached.size() == curSize && cached.lastModified() == curMod) {
                            sha256 = cached.sha256();
                            aiMeta = cached.aiMetadata();
                        } else {
                            sha256 = HashUtil.calculateSHA256(f);
                            aiMeta = AIManager.getInstance().analyzeFileWithCache(f, sha256);
                            fileStatsCache.put(fileName, new FileHashEntry(curSize, curMod, sha256, aiMeta));
                        }

                        scanned.add(new SharedFile(
                                fileName,
                                curSize,
                                sha256,
                                "LOCAL",
                                localHostName
                        ).withAiMetadata(aiMeta));
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
