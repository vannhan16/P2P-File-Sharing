package com.nhannv.p2p.ui;

import com.nhannv.p2p.MainApplication;
import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.DownloadManager;
import com.nhannv.p2p.network.FileSenderTask;
import com.nhannv.p2p.network.P2PServer;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import com.nhannv.p2p.util.FirewallUtil;
import com.nhannv.p2p.util.SharedFolderManager;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.chart.AreaChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.input.Dragboard;
import javafx.scene.input.TransferMode;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;
import javafx.util.Duration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controller trung tâm của {@code MainView.fxml} (Hoàn thiện toàn diện Milestone 1 -> Milestone 5).
 *
 * <h2>Điểm nhấn kỹ thuật mới ở Milestone 5 (Phục vụ thuyết trình &amp; vấn đáp đồ án)</h2>
 * <ol>
 *   <li><b>Kết nối xuyên Internet ({@code host:port}) &amp; Mã PIN bảo mật 4 chữ số:</b>
 *       Hỗ trợ nhập cả IP LAN, Radmin VPN ({@code 26.x.x.x}) lẫn TCP Tunnel ngoài Internet
 *       (ví dụ {@code 0.tcp.ap.ngrok.io:14523} hoặc Playit.gg). Tích hợp xác thực mã PIN 4 số
 *       trên các lệnh {@code DOWNLOAD_FILE}, {@code DOWNLOAD_CHUNK}, {@code PUSH_FILE}.</li>
 *   <li><b>Tự động đồng bộ thư mục chia sẻ ({@link java.nio.file.WatchService}):</b>
 *       Daemon Thread lắng nghe sự kiện {@code ENTRY_CREATE}, {@code ENTRY_DELETE}, {@code ENTRY_MODIFY}
 *       của hệ điều hành, tự động băm lại SHA-256 và cập nhật {@code sharedFileTable} qua {@code Platform.runLater()}.</li>
 *   <li><b>Kéo thả tệp (Drag &amp; Drop) &amp; Biểu đồ tốc độ thời gian thực ({@link AreaChart}):</b>
 *       Kéo thả tệp vào bảng giữa để chia sẻ ngay lập tức hoặc thả vào tên một Peer ở cột trái để Push tệp.
 *       Biểu đồ {@code AreaChart<Number, Number>} cập nhật tốc độ truyền tải (MB/s) mỗi giây với cửa sổ trượt 30 giây.</li>
 *   <li><b>Bảng Lịch sử truyền tải &amp; Xuất báo cáo hiệu năng CSV:</b>
 *       Ghi lại toàn bộ thông số phiên truyền tải (Thời gian, Tên tệp, Dung lượng, Peer, Chiều truyền,
 *       Tốc độ TB MB/s, Thời gian hoàn thành ms, Kết quả SHA-256) và cho phép xuất ra tệp {@code .csv}.</li>
 * </ol>
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int SPEED_WINDOW_SECONDS = 30;

    @FXML
    private Label networkIpBadgeLabel;

    @FXML
    private Label sharedFolderLabel;

    @FXML
    private Button firewallConfigButton;

    @FXML
    private Button themeToggleButton;

    // --- TÁC VỤ 1: Ô nhập host:port & Security PIN 4 chữ số ---
    @FXML
    private TextField directIpTextField;

    @FXML
    private Button connectDirectIpButton;

    @FXML
    private CheckBox pinEnableCheckBox;

    @FXML
    private TextField myPinTextField;

    @FXML
    private TextField peerPinTextField;

    @FXML
    private ListView<Peer> peerListView;

    @FXML
    private Label peerCountLabel;

    @FXML
    private Button deepScanButton;

    @FXML
    private TextField searchTextField;

    @FXML
    private Button chooseSharedFolderButton;

    @FXML
    private Button refreshFilesButton;

    @FXML
    private TableView<SharedFile> sharedFileTable;

    @FXML
    private TableColumn<SharedFile, String> fileNameColumn;

    @FXML
    private TableColumn<SharedFile, String> fileSizeColumn;

    @FXML
    private TableColumn<SharedFile, String> fileSourcesColumn;

    @FXML
    private TableColumn<SharedFile, String> fileOwnerColumn;

    @FXML
    private TableColumn<SharedFile, String> fileHashColumn;

    // --- TÁC VỤ 3: Biểu đồ tốc độ thời gian thực (AreaChart 30s) ---
    @FXML
    private AreaChart<Number, Number> speedTelemetryChart;

    @FXML
    private NumberAxis speedTimeAxis;

    @FXML
    private NumberAxis speedValueAxis;

    @FXML
    private TextArea networkLogArea;

    // --- TÁC VỤ 4: Bảng Lịch sử truyền tải & Xuất báo cáo CSV ---
    @FXML
    private Label historyCountLabel;

    @FXML
    private Button exportCsvButton;

    @FXML
    private TableView<TransferHistoryItem> historyTable;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyTimeColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyFileColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historySizeColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyPeerColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyDirectionColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historySpeedColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyDurationColumn;

    @FXML
    private TableColumn<TransferHistoryItem, String> historyHashStatusColumn;

    @FXML
    private Button downloadSelectedButton;

    @FXML
    private Button pauseButton;

    @FXML
    private Button resumeButton;

    @FXML
    private Button cancelButton;

    @FXML
    private Button chooseFileButton;

    @FXML
    private Label statusLabel;

    @FXML
    private ProgressBar transferProgressBar;

    /** Danh sách Peer đang online trong LAN / Radmin VPN / Internet Tunnel. */
    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();

    /** Danh sách tệp thô (chưa gộp) từ máy cục bộ và các Peer. */
    private final List<SharedFile> rawFileEntries = new ArrayList<>();

    /** Danh sách tệp đã gộp theo mã SHA-256 để hiển thị lên TableView. */
    private final ObservableList<SharedFile> masterFileList = FXCollections.observableArrayList();

    /** Danh sách bọc quanh masterFileList để lọc theo ô tìm kiếm tức thời. */
    private FilteredList<SharedFile> filteredFileList;

    /** Danh sách lưu lịch sử truyền tải tệp (Gửi / Nhận / Swarm) phục vụ Tab Thống kê & Xuất CSV. */
    private final ObservableList<TransferHistoryItem> historyList = FXCollections.observableArrayList();

    private final SharedFolderManager sharedFolderManager = new SharedFolderManager();
    private final DownloadManager downloadManager = new DownloadManager(this::appendNetworkLog);
    private DiscoveryService discoveryService;
    private P2PServer p2pServer;

    /** Lưu mẫu tốc độ truyền tải tức thời (MB/s) mới nhất từ luồng tải/gửi để vẽ lên AreaChart. */
    private final AtomicReference<Double> latestSpeedSampleMBps = new AtomicReference<>(0.0);

    /** Chuỗi dữ liệu (Series) của biểu đồ AreaChart và bộ đếm giây (Tick). */
    private XYChart.Series<Number, Number> speedSeries;
    private Timeline speedChartTimeline;
    private int chartElapsedSeconds = 0;

    @FXML
    private void initialize() {
        // --- 1. Cột trái: Khởi tạo mã PIN ngẫu nhiên 4 số & Danh sách Peer ---
        initializeSecurityPinControls();

        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer (LAN / Radmin / Tunnel)..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        peerListView.getSelectionModel().selectedItemProperty().addListener((obs, oldPeer, selectedPeer) -> {
            if (selectedPeer != null) {
                fetchFilesFromPeersAsync(List.of(selectedPeer));
            }
        });

        // Gắn tính năng Kéo thả tệp (Drag & Drop) trực tiếp vào từng dòng Peer trên peerListView
        setupPeerListDragAndDrop();

        // --- 2. Tab 1: Bảng tệp chia sẻ 5 cột + Kéo thả tệp vào bảng để chia sẻ ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        fileSourcesColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerCountDisplay()));
        fileOwnerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().ownerDisplay()));
        fileHashColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Hash()));
        sharedFileTable.setPlaceholder(new Label(
                "Chưa có tệp nào — Hãy kéo thả tệp từ Windows Explorer vào bảng này để chia sẻ ngay!"));

        filteredFileList = new FilteredList<>(masterFileList, file -> true);
        sharedFileTable.setItems(filteredFileList);

        searchTextField.textProperty().addListener((obs, oldQuery, newQuery) -> {
            String keyword = (newQuery == null) ? "" : newQuery.trim().toLowerCase(Locale.ROOT);
            filteredFileList.setPredicate(file -> matchesSearchKeyword(file, keyword));
        });

        // Gắn tính năng Kéo thả tệp (Drag & Drop) vào bảng sharedFileTable
        setupSharedTableDragAndDrop();

        // --- 3. Tab 1 (Dưới): Khởi tạo Biểu đồ tốc độ thời gian thực AreaChart (30 giây) ---
        initializeSpeedTelemetryChart();

        // --- 4. Tab 2: Khởi tạo Bảng Lịch sử truyền tải & Thống kê hiệu năng ---
        initializeHistoryTable();

        // Cấu hình callback cho DownloadManager (cung cấp PIN, mẫu tốc độ MB/s và lịch sử truyền tải)
        downloadManager.setAuthPinSupplier(this::getPeerAuthPinInput);
        downloadManager.setSpeedSampleCallback(this::recordSpeedSample);
        downloadManager.setHistoryCallback(this::recordTransferHistory);

        updateSharedFolderLabel();
        refreshHeaderIpBadges();
        appendNetworkLog("Khởi tạo hệ thống P2P File Sharing (M5: Tunnel host:port, Security PIN, WatchService, AreaChart & CSV)...");
        logDetectedPhysicalSubnets();
        checkFirewallStatusAsync();

        // --- 5. Khởi động UDP Discovery & P2P Multi-Command TCP Server ---
        startDiscovery();
        startP2PServer();

        // --- 6. Quét thư mục Shared lần đầu & Bật Java NIO WatchService tự động đồng bộ ---
        scanLocalSharedFolderAsync();
        startSharedFolderRealTimeWatcher();
    }

    // ------------------------------------------------------------------------------------------
    // TÁC VỤ 1: HỖ TRỢ KẾT NỐI XUYÊN INTERNET (HOST:PORT) & MÃ PIN BẢO MẬT 4 SỐ
    // ------------------------------------------------------------------------------------------

    /**
     * Sinh ngẫu nhiên một mã PIN 4 chữ số khi khởi động ứng dụng và gắn sự kiện đồng bộ
     * cấu hình bảo mật xuống {@link P2PServer}.
     */
    private void initializeSecurityPinControls() {
        String randomPin = String.format("%04d", ThreadLocalRandom.current().nextInt(10_000));
        if (myPinTextField != null) {
            myPinTextField.setText(randomPin);
        }
    }

    /**
     * Được gọi khi người dùng bật/tắt {@code CheckBox("Bật mã PIN bảo vệ")} hoặc sửa mã PIN.
     * Cập nhật ngay lập tức trạng thái khóa PIN trên {@link P2PServer}.
     */
    @FXML
    private void handlePinConfigChanged() {
        boolean enabled = pinEnableCheckBox != null && pinEnableCheckBox.isSelected();
        String pin = (myPinTextField == null || myPinTextField.getText() == null)
                ? ""
                : myPinTextField.getText().trim();

        if (p2pServer != null) {
            p2pServer.updateSecurityPinConfig(enabled, pin);
        }

        if (enabled) {
            appendNetworkLog("[BẢO MẬT PIN] Đã BẬT bảo vệ bằng mã PIN (" + pin
                    + "). Mọi yêu cầu tải/gửi tệp tới máy bạn phải kèm đúng mã PIN này.");
            setStatus("Đã bật mã PIN bảo vệ (" + pin + ").");
        } else {
            appendNetworkLog("[BẢO MẬT PIN] Đã TẮT mã PIN bảo vệ — cho phép truyền tệp tự do.");
            setStatus("Đã tắt mã PIN bảo vệ.");
        }
    }

    /**
     * Lấy mã PIN đối tác mà người dùng đã nhập ở ô {@code peerPinTextField} (nếu Peer đích có bật khóa PIN).
     */
    private String getPeerAuthPinInput() {
        if (peerPinTextField == null || peerPinTextField.getText() == null) {
            return "";
        }
        return peerPinTextField.getText().trim();
    }

    /**
     * Xử lý sự kiện khi người dùng nhập địa chỉ {@code host:port} (ví dụ: {@code 0.tcp.ap.ngrok.io:14523},
     * {@code 192.168.1.10:8889}, hoặc {@code 26.x.x.x}) và bấm nút <b>"Kết nối"</b>:
     * <ol>
     *   <li>Tách {@code host} và {@code port} qua {@link Peer#parseHostPort(String, int)} (mặc định cổng 8889 nếu không nhập {@code :port}).</li>
     *   <li>Nếu kết nối dùng cổng mặc định 8889 (LAN / Radmin VPN), gửi thêm gói UDP Direct Ping tới cổng 8888.</li>
     *   <li>Mở kết nối TCP tới {@code host:port} để xác thực {@code PING} và lấy danh sách tệp {@code LIST_FILES},
     *       lưu riêng {@code tcpPort} trong đối tượng {@link Peer} để hỗ trợ tải/gửi qua Internet Tunnel.</li>
     * </ol>
     */
    @FXML
    private void handleConnectDirectIp() {
        String rawInput = (directIpTextField == null) ? "" : directIpTextField.getText();
        String trimmedInput = (rawInput == null) ? "" : rawInput.trim();
        if (trimmedInput.isEmpty()) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa nhập địa chỉ kết nối");
            alert.setHeaderText("Vui lòng nhập IP hoặc host:port (Radmin VPN / Ngrok / Playit.gg / LAN)!");
            alert.setContentText("Ví dụ:\n• Radmin VPN / LAN: 26.45.12.88 hoặc 192.168.1.10:8889\n• Internet TCP Tunnel: 0.tcp.ap.ngrok.io:14523");
            alert.showAndWait();
            return;
        }

        Peer.HostPort target = Peer.parseHostPort(trimmedInput, P2PServer.DEFAULT_PORT);
        String targetEndpoint = target.toEndpointString();

        if (connectDirectIpButton != null) {
            connectDirectIpButton.setDisable(true);
        }
        appendNetworkLog("Đang kết nối trực tiếp tới " + targetEndpoint + " (Hỗ trợ LAN / Radmin VPN / TCP Tunnel)...");

        Task<List<SharedFile>> connectTask = new Task<>() {
            private Peer connectedPeer;

            @Override
            protected List<SharedFile> call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang kết nối TCP tới " + targetEndpoint + "...");

                // Bước 1: Nếu dùng cổng mặc định 8889 (LAN/Radmin), bắn thêm gói UDP Unicast PING tới cổng 8888
                if (discoveryService != null && target.port() == P2PServer.DEFAULT_PORT) {
                    discoveryService.sendDirectPing(target.host());
                }

                // Bước 2: Mở kết nối TCP tới host:port để lấy hostname và trạng thái khóa PIN
                if (discoveryService != null) {
                    connectedPeer = discoveryService.connectAndVerifyPeerTcp(
                            target.host(),
                            target.port(),
                            4_000
                    );
                }
                if (connectedPeer == null) {
                    connectedPeer = new Peer(
                            target.host(),
                            target.port(),
                            "Peer-" + target.host(),
                            System.currentTimeMillis(),
                            false
                    );
                }

                // Bước 3: Gửi lệnh LIST_FILES tới đúng host:port của Peer
                List<SharedFile> files = P2PServer.requestFileListFromPeer(connectedPeer);

                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã kết nối thành công tới " + connectedPeer
                        + " (" + files.size() + " tệp chia sẻ)");
                return files;
            }
        };

        connectTask.setOnSucceeded(e -> {
            if (connectDirectIpButton != null) {
                connectDirectIpButton.setDisable(false);
            }
            List<SharedFile> remoteFiles = connectTask.getValue();

            Peer peerToSelect = findPeerByEndpoint(target.host(), target.port());
            String ownerName = (remoteFiles != null && !remoteFiles.isEmpty())
                    ? remoteFiles.get(0).ownerName()
                    : ("Peer-" + target.host());
            if (peerToSelect == null) {
                peerToSelect = new Peer(
                        target.host(),
                        target.port(),
                        ownerName,
                        System.currentTimeMillis(),
                        false
                );
                peerList.add(peerToSelect);
            }
            peerListView.getSelectionModel().select(peerToSelect);

            rawFileEntries.removeIf(f ->
                    f.ownerIp().equalsIgnoreCase(targetEndpoint)
                            || f.ownerIp().equalsIgnoreCase(target.host()));
            if (remoteFiles != null && !remoteFiles.isEmpty()) {
                addOrUpdateFileEntries(remoteFiles, false);
            } else {
                rebuildMergedFileTable();
            }

            appendNetworkLog("Kết nối trực tiếp thành công tới " + peerToSelect
                    + " -> Nhận được " + (remoteFiles == null ? 0 : remoteFiles.size()) + " tệp chia sẻ.");
        });

        connectTask.setOnFailed(e -> {
            if (connectDirectIpButton != null) {
                connectDirectIpButton.setDisable(false);
            }
            Throwable ex = connectTask.getException();
            String reason = (ex != null && ex.getMessage() != null) ? ex.getMessage() : "Không phản hồi";
            String msg = "Không kết nối được tới " + targetEndpoint + " (" + reason
                    + "). Hãy kiểm tra địa chỉ host:port hoặc Firewall!";
            setStatus(msg);
            appendNetworkLog("LỖI kết nối trực tiếp tới " + targetEndpoint + ": " + msg);
        });

        bindUiToTask(connectTask);
        Thread t = new Thread(connectTask, "p2p-direct-endpoint-connector");
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------------------------------------
    // TÁC VỤ 2: TỰ ĐỘNG ĐỒNG BỘ THƯ MỤC CHIA SẺ (JAVA NIO WATCHSERVICE)
    // ------------------------------------------------------------------------------------------

    /**
     * Khởi chạy {@link java.nio.file.WatchService} trên luồng ngầm (Daemon Thread) thông qua
     * {@link SharedFolderManager#startRealTimeWatcher}.
     *
     * <p><b>Giải thích cơ chế cho buổi bảo vệ đồ án:</b>
     * <ul>
     *   <li>Khi người dùng thêm tệp mới, xóa tệp hoặc chỉnh sửa tệp trong thư mục {@code Shared}
     *       ngoài Windows Explorer, Kernel Windows gửi sự kiện {@code ENTRY_CREATE}, {@code ENTRY_DELETE},
     *       {@code ENTRY_MODIFY} cho {@code WatchService}.</li>
     *   <li>Luồng {@code WatchService} tự động tính lại mã băm SHA-256 trên background thread,
     *       sau đó gọi callback này để cập nhật {@code sharedFileTable} trên luồng giao diện bằng
     *       {@link Platform#runLater(Runnable)} mà không cần bấm nút "Làm mới".</li>
     * </ul>
     */
    private void startSharedFolderRealTimeWatcher() {
        sharedFolderManager.startRealTimeWatcher((updatedLocalFiles, watchEventDescription) ->
                runOnFxThread(() -> {
                    rawFileEntries.removeIf(SharedFile::isLocal);
                    if (updatedLocalFiles != null) {
                        rawFileEntries.addAll(0, updatedLocalFiles);
                    }
                    rebuildMergedFileTable();
                    appendNetworkLog(watchEventDescription);
                    setStatus(watchEventDescription);
                })
        );
        appendNetworkLog("Đã bật Java NIO WatchService theo dõi thời gian thực thư mục: "
                + sharedFolderManager.getSharedFolderPath());
    }

    // ------------------------------------------------------------------------------------------
    // TÁC VỤ 3: KÉO THẢ TỆP (DRAG & DROP) & BIỂU ĐỒ TỐC ĐỘ THỜI GIAN THỰC (AREACHART)
    // ------------------------------------------------------------------------------------------

    /**
     * Gắn sự kiện Kéo thả tệp ({@code onDragOver} và {@code onDragDropped}) vào bảng {@code sharedFileTable}.
     * Khi người dùng kéo một hoặc nhiều tệp từ ngoài màn hình Desktop / Windows Explorer thả vào bảng:
     * <ul>
     *   <li>Sao chép tệp vào thư mục {@code Shared} trên luồng nền (không làm đơ giao diện).</li>
     *   <li>Băm SHA-256 và hiển thị ngay lên bảng chia sẻ (cùng với sự hỗ trợ của {@code WatchService}).</li>
     * </ul>
     */
    private void setupSharedTableDragAndDrop() {
        sharedFileTable.setOnDragOver(event -> {
            Dragboard db = event.getDragboard();
            if (db.hasFiles()) {
                event.acceptTransferModes(TransferMode.COPY);
            }
            event.consume();
        });

        sharedFileTable.setOnDragDropped(event -> {
            Dragboard db = event.getDragboard();
            boolean success = false;
            if (db.hasFiles()) {
                List<File> droppedFiles = new ArrayList<>(db.getFiles());
                success = true;
                handleFilesDroppedIntoSharedTable(droppedFiles);
            }
            event.setDropCompleted(success);
            event.consume();
        });
    }

    private void handleFilesDroppedIntoSharedTable(List<File> droppedFiles) {
        appendNetworkLog("Kéo thả (Drag & Drop): Đang sao chép " + droppedFiles.size()
                + " tệp vào thư mục chia sẻ (" + sharedFolderManager.getSharedFolderPath() + ")...");

        Task<List<SharedFile>> copyTask = new Task<>() {
            @Override
            protected List<SharedFile> call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang sao chép và băm SHA-256 cho "
                        + droppedFiles.size() + " tệp vừa kéo thả...");
                int copiedCount = sharedFolderManager.copyDroppedFilesToSharedFolder(droppedFiles);
                List<SharedFile> updatedLocal = sharedFolderManager.scanAndHashSync();
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã thêm " + copiedCount
                        + " tệp mới vào thư mục chia sẻ qua Kéo & Thả!");
                return updatedLocal;
            }
        };

        copyTask.setOnSucceeded(e -> {
            List<SharedFile> localFiles = copyTask.getValue();
            rawFileEntries.removeIf(SharedFile::isLocal);
            if (localFiles != null) {
                rawFileEntries.addAll(0, localFiles);
            }
            rebuildMergedFileTable();
            appendNetworkLog("Kéo thả hoàn tất: Thư mục chia sẻ hiện có "
                    + (localFiles == null ? 0 : localFiles.size()) + " tệp.");
        });

        copyTask.setOnFailed(e -> {
            Throwable ex = copyTask.getException();
            String err = (ex != null) ? ex.getMessage() : "Lỗi I/O";
            appendNetworkLog("Lỗi khi sao chép tệp kéo thả: " + err);
            setStatus("Lỗi sao chép tệp kéo thả: " + err);
        });

        bindUiToTask(copyTask);
        Thread t = new Thread(copyTask, "p2p-drag-drop-copy-worker");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Gắn sự kiện Kéo thả tệp ({@code onDragOver} và {@code onDragDropped}) vào từng ô {@link Peer}
     * trên {@code peerListView}. Nếu người dùng kéo tệp từ Windows Explorer và thả thẳng vào tên một Peer
     * ở cột bên trái, ứng dụng lập tức kích hoạt lệnh {@code PUSH_FILE} gửi tệp đó tới thẳng Peer đó.
     */
    private void setupPeerListDragAndDrop() {
        peerListView.setCellFactory(listView -> {
            ListCell<Peer> cell = new ListCell<>() {
                @Override
                protected void updateItem(Peer item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                        setStyle("");
                    } else {
                        setText(item.toString());
                    }
                }
            };

            cell.setOnDragOver(event -> {
                if (!cell.isEmpty() && cell.getItem() != null && event.getDragboard().hasFiles()) {
                    event.acceptTransferModes(TransferMode.COPY);
                    cell.setStyle("-fx-background-color: rgba(52, 211, 153, 0.30); -fx-font-weight: bold;");
                }
                event.consume();
            });

            cell.setOnDragExited(event -> {
                cell.setStyle("");
                event.consume();
            });

            cell.setOnDragDropped(event -> {
                cell.setStyle("");
                Peer targetPeer = cell.getItem();
                Dragboard db = event.getDragboard();
                boolean success = false;
                if (targetPeer != null && db.hasFiles()) {
                    List<File> files = db.getFiles();
                    for (File file : files) {
                        if (file.isFile()) {
                            pushFileToPeer(file, targetPeer);
                            success = true;
                        }
                    }
                }
                event.setDropCompleted(success);
                event.consume();
            });

            return cell;
        });
    }

    /**
     * Thực hiện đẩy (Push) trực tiếp một tệp tới {@link Peer} được chỉ định (kèm theo mã PIN xác thực nếu có).
     */
    private void pushFileToPeer(File file, Peer targetPeer) {
        String authPin = getPeerAuthPinInput();
        appendNetworkLog("Đang Push tệp \"" + file.getName() + "\" tới " + targetPeer.endpoint() + "...");

        FileSenderTask senderTask = new FileSenderTask(
                file,
                targetPeer.endpoint(),
                authPin,
                this::recordSpeedSample,
                this::recordTransferHistory,
                this::appendNetworkLog
        );

        senderTask.setOnSucceeded(e -> appendNetworkLog(
                "Đã Push thành công tệp \"" + file.getName() + "\" tới " + targetPeer.endpoint()));
        senderTask.setOnFailed(e -> {
            Throwable ex = senderTask.getException();
            String reason = (ex != null && ex.getMessage() != null) ? ex.getMessage() : "Lỗi kết nối";
            appendNetworkLog("Push tệp \"" + file.getName() + "\" thất bại: " + reason);
            if (reason.contains("PIN") || reason.contains("AUTH_FAILED")) {
                showPinAuthFailedAlert(targetPeer.endpoint(), reason);
            }
        });

        bindUiToTask(senderTask);
        Thread senderThread = new Thread(senderTask, "p2p-tcp-file-sender");
        senderThread.setDaemon(true);
        senderThread.start();
    }

    /**
     * Khởi tạo biểu đồ tốc độ truyền tải thời gian thực ({@link AreaChart}) với cửa sổ trượt 30 giây.
     *
     * <p><b>Giải thích nguyên lý hoạt động của {@code AreaChart} (Dùng khi thuyết trình đồ án):</b>
     * <ol>
     *   <li>Các luồng truyền tệp ({@code ResumableDownloadTask}, {@code SwarmDownloadTask}, {@code FileSenderTask},
     *       {@code P2PServer}) tính toán tốc độ tức thời {@code MB/s} mỗi 200ms và ghi vào biến nguyên tử
     *       {@link #latestSpeedSampleMBps}.</li>
     *   <li>Một bộ đếm thời gian {@link Timeline} chạy định kỳ mỗi 1 giây trên JavaFX Application Thread
     *       đọc giá trị tốc độ hiện tại (và tự động reset về {@code 0.0 MB/s} cho chu kỳ kế tiếp nếu không có truyền tải).</li>
     *   <li>Điểm dữ liệu mới {@code (giây_thứ_N, tốc_độ_MBps)} được thêm vào {@code XYChart.Series}.
     *       Khi số điểm vượt quá 30 giây ({@link #SPEED_WINDOW_SECONDS}), điểm cũ nhất ở đầu danh sách bị xóa
     *       và trục hoành {@code speedTimeAxis} dịch chuyển {@code lowerBound = N - 30}, {@code upperBound = N}
     *       tạo hiệu ứng biểu đồ trượt mượt mà như Task Manager của Windows.</li>
     * </ol>
     */
    private void initializeSpeedTelemetryChart() {
        if (speedTelemetryChart == null || speedTimeAxis == null) {
            return;
        }
        speedSeries = new XYChart.Series<>();
        speedSeries.setName("Tốc độ truyền tải (MB/s)");
        speedTelemetryChart.getData().clear();
        speedTelemetryChart.getData().add(speedSeries);

        // Khởi tạo điểm 0 ban đầu
        speedSeries.getData().add(new XYChart.Data<>(0, 0.0));

        speedChartTimeline = new Timeline(new KeyFrame(Duration.seconds(1.0), event -> {
            chartElapsedSeconds++;
            // Lấy tốc độ MB/s mới nhất trong 1 giây qua và đặt lại về 0.0 cho giây tiếp theo
            double currentSpeedMBps = latestSpeedSampleMBps.getAndSet(0.0);
            double roundedSpeed = Math.round(currentSpeedMBps * 100.0) / 100.0;

            speedSeries.getData().add(new XYChart.Data<>(chartElapsedSeconds, roundedSpeed));

            // Giữ tối đa 31 điểm dữ liệu (cửa sổ trượt 30 giây gần nhất) để không tốn RAM
            if (speedSeries.getData().size() > SPEED_WINDOW_SECONDS + 1) {
                speedSeries.getData().remove(0);
            }

            if (chartElapsedSeconds > SPEED_WINDOW_SECONDS) {
                speedTimeAxis.setLowerBound(chartElapsedSeconds - SPEED_WINDOW_SECONDS);
                speedTimeAxis.setUpperBound(chartElapsedSeconds);
            }
        }));
        speedChartTimeline.setCycleCount(Timeline.INDEFINITE);
        speedChartTimeline.play();
    }

    /**
     * Nhận mẫu tốc độ tức thời (MB/s) từ bất kỳ luồng tải hoặc gửi tệp nào.
     * Giữ lại giá trị cao nhất trong chu kỳ 1 giây để hiển thị chính xác đỉnh tốc độ trên biểu đồ.
     */
    private void recordSpeedSample(double speedMBps) {
        if (speedMBps < 0) {
            return;
        }
        latestSpeedSampleMBps.accumulateAndGet(speedMBps, Math::max);
    }

    // ------------------------------------------------------------------------------------------
    // TÁC VỤ 4: BẢNG LỊCH SỬ TRUYỀN TẢI & XUẤT BÁO CÁO HIỆU NĂNG CSV
    // ------------------------------------------------------------------------------------------

    /**
     * Khởi tạo các cột cho bảng {@code historyTable} ở Tab "Lịch sử & Thống kê".
     */
    private void initializeHistoryTable() {
        if (historyTable == null) {
            return;
        }
        historyTimeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().timestamp()));
        historyFileColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        historySizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        historyPeerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerEndpoint()));
        historyDirectionColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().direction()));
        historySpeedColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSpeed()));
        historyDurationColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedDuration()));
        historyHashStatusColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Status()));

        historyTable.setPlaceholder(new Label("Chưa có phiên truyền tải nào hoàn tất trong phiên làm việc này."));
        historyTable.setItems(historyList);

        if (historyCountLabel != null) {
            historyCountLabel.textProperty().bind(
                    Bindings.size(historyList).asString("(%d phiên truyền tải đã ghi nhận)"));
        }
    }

    /**
     * Ghi nhận một bản ghi {@link TransferHistoryItem} mới vào đầu bảng lịch sử trên luồng UI.
     */
    private void recordTransferHistory(TransferHistoryItem item) {
        if (item == null) {
            return;
        }
        runOnFxThread(() -> historyList.add(0, item));
    }

    /**
     * Xử lý sự kiện khi người dùng bấm nút <b>"Xuất báo cáo CSV"</b> ở Tab "Lịch sử & Thống kê".
     * Xuất toàn bộ dữ liệu trong {@code historyList} ra tệp {@code .csv} chuẩn UTF-8 (có BOM để mở trực tiếp
     * bằng Microsoft Excel hiển thị đúng tiếng Việt không lỗi font).
     */
    @FXML
    private void handleExportCsv() {
        if (historyList.isEmpty()) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa có dữ liệu thống kê");
            alert.setHeaderText("Bảng lịch sử truyền tải hiện đang trống!");
            alert.setContentText("Hãy thực hiện tải hoặc gửi ít nhất 1 tệp trước khi xuất báo cáo CSV.");
            alert.showAndWait();
            return;
        }

        FileChooser chooser = new FileChooser();
        chooser.setTitle("Lưu báo cáo hiệu năng truyền tải P2P (CSV)");
        String defaultName = "P2P_Transfer_Report_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".csv";
        chooser.setInitialFileName(defaultName);
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("CSV Files (*.csv)", "*.csv"));

        Window owner = (exportCsvButton != null && exportCsvButton.getScene() != null)
                ? exportCsvButton.getScene().getWindow()
                : null;
        File targetFile = chooser.showSaveDialog(owner);
        if (targetFile == null) {
            return;
        }

        try (FileOutputStream fos = new FileOutputStream(targetFile);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
            // Ghi UTF-8 BOM (0xEF, 0xBB, 0xBF) để Microsoft Excel nhận diện đúng tiếng Việt có dấu
            fos.write(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});

            writer.write(TransferHistoryItem.csvHeader());
            writer.newLine();
            for (TransferHistoryItem item : historyList) {
                writer.write(item.toCsvRow());
                writer.newLine();
            }
            writer.flush();

            String msg = "Đã xuất báo cáo hiệu năng (" + historyList.size() + " dòng) ra tệp: "
                    + targetFile.getAbsolutePath();
            appendNetworkLog(msg);
            setStatus(msg);

            Alert info = new Alert(Alert.AlertType.INFORMATION);
            info.setTitle("Xuất báo cáo CSV thành công");
            info.setHeaderText("Đã lưu tệp báo cáo thống kê hiệu năng truyền tải!");
            info.setContentText("Đường dẫn tệp:\n" + targetFile.getAbsolutePath()
                    + "\n\nBạn có thể mở trực tiếp bằng Microsoft Excel hoặc Google Sheets để đưa vào báo cáo đồ án.");
            info.showAndWait();
        } catch (IOException e) {
            String err = "Lỗi khi xuất tệp CSV: " + e.getMessage();
            appendNetworkLog(err);
            setStatus(err);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Khởi tạo mạng & Các sự kiện giao diện cốt lõi
    // ------------------------------------------------------------------------------------------

    private void refreshHeaderIpBadges() {
        String lanIp = DiscoveryService.detectLocalLanIp();
        String radminIp = DiscoveryService.detectRadminVpnIp();
        if (networkIpBadgeLabel != null) {
            networkIpBadgeLabel.setText("IP LAN: " + lanIp + "   |   IP Radmin VPN: " + radminIp);
        }
    }

    private void logDetectedPhysicalSubnets() {
        List<DiscoveryService.LanSubnetTarget> subnets = DiscoveryService.collectPhysicalLanSubnets();
        if (subnets.isEmpty()) {
            appendNetworkLog("Cảnh báo: Chưa phát hiện card mạng LAN/Radmin VPN nào đang có IPv4.");
        } else {
            for (DiscoveryService.LanSubnetTarget s : subnets) {
                String tag = s.isRadmin() ? "[RADMIN VPN WHITELIST]" : "[LAN VẬT LÝ]";
                appendNetworkLog(tag + " " + s.interfaceName()
                        + " [IP=" + s.localAddress().getHostAddress()
                        + " -> Broadcast=" + s.broadcastAddress().getHostAddress() + "]");
            }
        }
    }

    private void checkFirewallStatusAsync() {
        Thread t = new Thread(() -> {
            boolean configured = FirewallUtil.isFirewallRuleConfigured();
            runOnFxThread(() -> {
                if (configured) {
                    if (firewallConfigButton != null) {
                        firewallConfigButton.setText("Firewall: Đã mở Port (8888/8889)");
                    }
                    appendNetworkLog("Windows Firewall: Đã có sẵn Rule (profile=any) cho phép UDP 8888 & TCP 8889.");
                } else {
                    appendNetworkLog("Gợi ý: Hãy bấm 'Cấu hình Firewall (1-Click)' ở góc trên để mở cổng cho Radmin VPN & LAN!");
                }
            });
        }, "p2p-firewall-checker");
        t.setDaemon(true);
        t.start();
    }

    private static boolean matchesSearchKeyword(SharedFile file, String keyword) {
        if (keyword.isEmpty()) {
            return true;
        }
        return file.fileName().toLowerCase(Locale.ROOT).contains(keyword)
                || file.ownerName().toLowerCase(Locale.ROOT).contains(keyword)
                || file.ownerIp().toLowerCase(Locale.ROOT).contains(keyword)
                || file.sha256Hash().toLowerCase(Locale.ROOT).contains(keyword);
    }

    /** Khởi động dịch vụ UDP Discovery trên cổng 8888. */
    private void startDiscovery() {
        discoveryService = new DiscoveryService(this);
        try {
            discoveryService.start();
            appendNetworkLog("UDP Discovery đã mở trên cổng " + discoveryService.getPort()
                    + " (Ưu tiên Whitelist Radmin VPN 26.x.x.x + Card LAN vật lý).");
        } catch (BindException e) {
            String msg = "Lỗi: UDP port " + discoveryService.getPort() + " đang bị chiếm!";
            setStatus(msg);
            appendNetworkLog(msg);
        } catch (IOException e) {
            String msg = "Lỗi: không khởi động được Discovery - " + e.getMessage();
            setStatus(msg);
            appendNetworkLog(msg);
        }
    }

    /** Khởi động {@link P2PServer} đa lệnh trên cổng TCP 8889 và bind vào UI. */
    private void startP2PServer() {
        p2pServer = new P2PServer(
                P2PServer.DEFAULT_PORT,
                sharedFolderManager,
                receivedFile -> runOnFxThread(() -> addOrUpdateFileEntries(List.of(receivedFile), false)),
                this::appendNetworkLog
        );
        p2pServer.setDiscoveryService(discoveryService);
        p2pServer.setSpeedSampleCallback(this::recordSpeedSample);
        p2pServer.setHistoryCallback(this::recordTransferHistory);

        // Đồng bộ cấu hình mã PIN hiện tại trên giao diện xuống P2PServer
        boolean pinEnabled = pinEnableCheckBox != null && pinEnableCheckBox.isSelected();
        String currentPin = (myPinTextField != null) ? myPinTextField.getText() : "";
        p2pServer.updateSecurityPinConfig(pinEnabled, currentPin);

        bindUiToTask(p2pServer);
        p2pServer.messageProperty().addListener((obs, oldMsg, newMsg) -> {
            if (!downloadManager.isDownloading()) {
                bindUiToTask(p2pServer);
            }
        });

        Thread serverThread = new Thread(p2pServer, "p2p-multi-command-server");
        serverThread.setDaemon(true);
        serverThread.start();
        appendNetworkLog("TCP Multi-Command Server lắng nghe cổng " + P2PServer.DEFAULT_PORT
                + " (PING, LIST_FILES, DOWNLOAD_FILE, DOWNLOAD_CHUNK, PUSH_FILE + Security PIN).");
    }

    /**
     * Giải phóng toàn bộ tài nguyên mạng, đóng {@link java.nio.file.WatchService} và dừng
     * {@link Timeline} biểu đồ khi người dùng đóng ứng dụng ({@code MainApplication.stop()}).
     */
    public void shutdown() {
        if (speedChartTimeline != null) {
            speedChartTimeline.stop();
        }
        sharedFolderManager.stopWatcher();
        if (discoveryService != null) {
            discoveryService.stop();
        }
        if (p2pServer != null) {
            p2pServer.stopServer();
        }
    }

    // ------------------------------------------------------------------------------------------
    // PeerDiscoveryListener
    // ------------------------------------------------------------------------------------------

    @Override
    public void onPeerDiscovered(Peer peer) {
        runOnFxThread(() -> {
            int index = indexOfPeer(peer);
            if (index >= 0) {
                peerList.set(index, peer);
            } else {
                peerList.add(peer);
                appendNetworkLog("Phát hiện Peer mới: " + peer);
            }
            fetchFilesFromPeersAsync(List.of(peer));
        });
    }

    @Override
    public void onPeerLost(Peer peer) {
        runOnFxThread(() -> {
            peerList.removeIf(p -> p.isSamePeer(peer));
            rawFileEntries.removeIf(f ->
                    f.ownerIp().equalsIgnoreCase(peer.ipAddress())
                            || f.ownerIp().equalsIgnoreCase(peer.endpoint()));
            rebuildMergedFileTable();
            appendNetworkLog("Peer đã offline: " + peer);
        });
    }

    @Override
    public void onDiscoveryError(String message, Throwable cause) {
        runOnFxThread(() -> {
            setStatus("Lỗi mạng: " + message);
            appendNetworkLog("Cảnh báo mạng: " + message);
        });
    }

    // ------------------------------------------------------------------------------------------
    // Các sự kiện nút bấm trên giao diện
    // ------------------------------------------------------------------------------------------

    @FXML
    private void handleConfigureFirewall() {
        if (firewallConfigButton != null) {
            firewallConfigButton.setDisable(true);
        }
        appendNetworkLog("Đang yêu cầu quyền Administrator (UAC) để mở cổng UDP 8888 & TCP 8889 (profile=any cho cả Radmin VPN & LAN)...");
        setStatus("Vui lòng bấm 'Yes' trên hộp thoại UAC của Windows để mở cổng mạng...");

        Task<Boolean> fwTask = new Task<>() {
            @Override
            protected Boolean call() {
                return FirewallUtil.configureFirewallRulesWithUac(
                        DiscoveryService.DEFAULT_PORT,
                        P2PServer.DEFAULT_PORT
                );
            }
        };

        fwTask.setOnSucceeded(e -> {
            if (firewallConfigButton != null) {
                firewallConfigButton.setDisable(false);
            }
            boolean ok = Boolean.TRUE.equals(fwTask.getValue());
            if (ok) {
                if (firewallConfigButton != null) {
                    firewallConfigButton.setText("Firewall: Đã mở Port (8888/8889)");
                }
                String msg = "Đã thêm Rule Inbound & Outbound (profile=any: Domain, Private, Public/Radmin VPN) vào Windows Firewall!";
                setStatus(msg);
                appendNetworkLog(msg);

                Alert alert = new Alert(Alert.AlertType.INFORMATION);
                alert.setTitle("Cấu hình Windows Firewall thành công");
                alert.setHeaderText("Đã mở cổng UDP 8888 & TCP 8889 cho cả mạng LAN và Radmin VPN!");
                alert.setContentText("Rule đã được áp dụng với profile=any (Private, Public, Domain). Hai máy có thể kết nối trực tiếp mà không cần tắt Firewall.");
                alert.showAndWait();
            } else {
                String msg = "Chưa thể thêm Rule Firewall (bạn đã hủy hộp thoại UAC hoặc không có quyền Admin).";
                setStatus(msg);
                appendNetworkLog(msg);
            }
        });

        fwTask.setOnFailed(e -> {
            if (firewallConfigButton != null) {
                firewallConfigButton.setDisable(false);
            }
            setStatus("Lỗi khi cấu hình Firewall.");
        });

        Thread t = new Thread(fwTask, "p2p-firewall-uac-config");
        t.setDaemon(true);
        t.start();
    }

    @FXML
    private void handleDeepScanLan() {
        refreshHeaderIpBadges();
        if (discoveryService == null) {
            return;
        }
        if (deepScanButton != null) {
            deepScanButton.setDisable(true);
            deepScanButton.setText("Đang quét dải IP (.1-.254)...");
        }

        Task<List<Peer>> scanTask = new Task<>() {
            @Override
            protected List<Peer> call() {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang quét sâu toàn bộ lớp mạng LAN / Radmin (50 luồng TCP, timeout 150ms)...");
                List<Peer> found = discoveryService.scanSubnetActiveTcp(
                        P2PServer.DEFAULT_PORT,
                        MainController.this::appendNetworkLog
                );
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Quét sâu hoàn tất — phát hiện "
                        + found.size() + " Peer đang mở cổng TCP " + P2PServer.DEFAULT_PORT + ".");
                return found;
            }
        };

        scanTask.setOnSucceeded(e -> {
            if (deepScanButton != null) {
                deepScanButton.setDisable(false);
                deepScanButton.setText("Quét sâu mạng LAN / Radmin");
            }
            List<Peer> foundPeers = scanTask.getValue();
            appendNetworkLog("Quét sâu TCP hoàn tất: tìm thấy "
                    + (foundPeers == null ? 0 : foundPeers.size()) + " Peer hoạt động.");
            if (foundPeers != null && !foundPeers.isEmpty()) {
                fetchFilesFromPeersAsync(foundPeers);
            }
        });

        scanTask.setOnFailed(e -> {
            if (deepScanButton != null) {
                deepScanButton.setDisable(false);
                deepScanButton.setText("Quét sâu mạng LAN / Radmin");
            }
            setStatus("Lỗi khi quét sâu mạng LAN.");
        });

        bindUiToTask(scanTask);
        Thread t = new Thread(scanTask, "p2p-deep-subnet-scan");
        t.setDaemon(true);
        t.start();
    }

    @FXML
    private void handleToggleTheme() {
        boolean isDark = MainApplication.toggleTheme();
        if (themeToggleButton != null) {
            themeToggleButton.setText(isDark ? "Primer Light" : "Nord Dark");
        }
        appendNetworkLog("Đã chuyển giao diện sang theme AtlantaFX: " + (isDark ? "Nord Dark" : "Primer Light"));
    }

    @FXML
    private void handleChooseSharedFolder() {
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Chọn thư mục chia sẻ (Shared Folder)");
        File currentDir = sharedFolderManager.getSharedFolderPath().toFile();
        if (currentDir.exists() && currentDir.isDirectory()) {
            chooser.setInitialDirectory(currentDir);
        }

        Window owner = chooseSharedFolderButton.getScene().getWindow();
        File selectedDir = chooser.showDialog(owner);
        if (selectedDir == null) {
            return;
        }

        sharedFolderManager.setSharedFolder(selectedDir);
        updateSharedFolderLabel();
        appendNetworkLog("Đã đổi thư mục chia sẻ sang: " + selectedDir.getAbsolutePath()
                + " (WatchService đã tự động chuyển sang thư mục mới)");
        scanLocalSharedFolderAsync();
    }

    @FXML
    private void handleRefreshFiles() {
        refreshHeaderIpBadges();
        List<Peer> snapshotPeers = new ArrayList<>(peerList);
        appendNetworkLog("Đang làm mới danh sách tệp từ thư mục chia sẻ và " + snapshotPeers.size() + " Peer...");

        Task<List<SharedFile>> refreshTask = new Task<>() {
            @Override
            protected List<SharedFile> call() {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang quét thư mục chia sẻ và truy vấn "
                        + snapshotPeers.size() + " Peer...");

                List<SharedFile> combined = new ArrayList<>(sharedFolderManager.scanAndHashSync());
                int queried = 0;
                for (Peer peer : snapshotPeers) {
                    if (isCancelled()) {
                        break;
                    }
                    queried++;
                    updateMessage("Trạng thái: Đang gửi LIST_FILES tới " + peer + " ("
                            + queried + "/" + snapshotPeers.size() + ")...");
                    try {
                        combined.addAll(P2PServer.requestFileListFromPeer(peer));
                    } catch (IOException ignored) {
                    }
                }

                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã làm mới danh sách (" + combined.size()
                        + " mục từ máy bạn và " + snapshotPeers.size() + " Peer)");
                return combined;
            }
        };

        refreshTask.setOnSucceeded(event -> {
            List<SharedFile> files = refreshTask.getValue();
            if (files != null) {
                addOrUpdateFileEntries(files, true);
                appendNetworkLog("Làm mới hoàn tất: tổng hợp được " + masterFileList.size() + " tệp duy nhất.");
            }
        });

        bindUiToTask(refreshTask);
        Thread thread = new Thread(refreshTask, "p2p-refresh-files");
        thread.setDaemon(true);
        thread.start();
    }

    @FXML
    private void handleDownloadSelectedFile() {
        SharedFile selectedFile = sharedFileTable.getSelectionModel().getSelectedItem();
        if (selectedFile == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn tệp để tải");
            alert.setHeaderText("Vui lòng chọn một tệp trong bảng danh sách ở giữa!");
            alert.setContentText("Hãy bấm chọn một tệp từ danh sách trước khi bấm \"Tải tệp đã chọn\".");
            alert.showAndWait();
            return;
        }

        Task<Boolean> downloadTask = downloadManager.startDownload(selectedFile);
        attachDownloadAuthFailureAlert(downloadTask, selectedFile.ownerDisplay());
        bindUiToTask(downloadTask);
    }

    @FXML
    private void handlePauseDownload() {
        boolean paused = downloadManager.pauseCurrentDownload();
        if (!paused) {
            setStatus("Không có tác vụ tải nào đang chạy để tạm dừng.");
        }
    }

    @FXML
    private void handleResumeDownload() {
        Task<Boolean> resumedTask = downloadManager.resumeCurrentDownload();
        if (resumedTask != null) {
            attachDownloadAuthFailureAlert(resumedTask, "Peer");
            bindUiToTask(resumedTask);
        } else {
            setStatus("Không có phiên tải gián đoạn nào (.part) để tiếp tục.");
        }
    }

    @FXML
    private void handleCancelDownload() {
        downloadManager.cancelCurrentDownload();
        transferProgressBar.progressProperty().unbind();
        transferProgressBar.setProgress(0.0);
        setStatus("Đã hủy tải và xóa tệp tạm (.part).");
    }

    @FXML
    private void handleChooseFile() {
        Peer selectedPeer = peerListView.getSelectionModel().getSelectedItem();
        if (selectedPeer == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn máy nhận (Peer)");
            alert.setHeaderText("Vui lòng chọn một Peer đang online ở danh sách bên trái!");
            alert.setContentText("Bạn cũng có thể kéo thả tệp trực tiếp từ Windows Explorer vào tên Peer ở cột bên trái.");
            alert.showAndWait();
            return;
        }

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để gửi tới " + selectedPeer);
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Tất cả tệp", "*.*"));

        Window owner = chooseFileButton.getScene().getWindow();
        File selectedFile = fileChooser.showOpenDialog(owner);
        if (selectedFile == null) {
            return;
        }

        pushFileToPeer(selectedFile, selectedPeer);
    }

    @FXML
    private void onChooseFileClicked() {
        handleChooseFile();
    }

    private void attachDownloadAuthFailureAlert(Task<Boolean> task, String endpointLabel) {
        task.exceptionProperty().addListener((obs, oldEx, newEx) -> {
            if (newEx != null && newEx.getMessage() != null
                    && (newEx.getMessage().contains("AUTH_FAILED") || newEx.getMessage().contains("PIN"))) {
                runOnFxThread(() -> showPinAuthFailedAlert(endpointLabel, newEx.getMessage()));
            }
        });
    }

    private void showPinAuthFailedAlert(String endpoint, String detailMessage) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Xác thực mã PIN thất bại (AUTH_FAILED)");
        alert.setHeaderText("Peer " + endpoint + " từ chối yêu cầu do sai hoặc thiếu mã PIN bảo vệ!");
        alert.setContentText(detailMessage
                + "\n\nHướng dẫn: Hãy hỏi mã PIN 4 chữ số của đối tác và nhập vào ô 'PIN đối tác' ở cột bên trái.");
        alert.show();
    }

    // ------------------------------------------------------------------------------------------
    // Gộp tệp trùng SHA-256 (Swarm Grouping) & Nhật ký sự kiện mạng
    // ------------------------------------------------------------------------------------------

    private void scanLocalSharedFolderAsync() {
        Task<List<SharedFile>> scanTask = sharedFolderManager.createScanTask();
        scanTask.setOnSucceeded(e -> {
            List<SharedFile> localFiles = scanTask.getValue();
            rawFileEntries.removeIf(SharedFile::isLocal);
            if (localFiles != null) {
                rawFileEntries.addAll(0, localFiles);
                appendNetworkLog("Đã băm SHA-256 xong " + localFiles.size()
                        + " tệp trong thư mục chia sẻ cục bộ.");
            }
            rebuildMergedFileTable();
        });
        bindUiToTask(scanTask);

        Thread scanThread = new Thread(scanTask, "p2p-shared-folder-scanner");
        scanThread.setDaemon(true);
        scanThread.start();
    }

    private void fetchFilesFromPeersAsync(List<Peer> peersToQuery) {
        if (peersToQuery == null || peersToQuery.isEmpty()) {
            return;
        }
        Task<List<SharedFile>> listTask = new Task<>() {
            @Override
            protected List<SharedFile> call() {
                List<SharedFile> fetched = new ArrayList<>();
                for (Peer peer : peersToQuery) {
                    try {
                        fetched.addAll(P2PServer.requestFileListFromPeer(peer));
                    } catch (IOException ignored) {
                    }
                }
                return fetched;
            }
        };

        listTask.setOnSucceeded(e -> {
            List<SharedFile> remoteFiles = listTask.getValue();
            if (remoteFiles != null && !remoteFiles.isEmpty()) {
                for (Peer p : peersToQuery) {
                    rawFileEntries.removeIf(f ->
                            f.ownerIp().equalsIgnoreCase(p.ipAddress())
                                    || f.ownerIp().equalsIgnoreCase(p.endpoint()));
                }
                addOrUpdateFileEntries(remoteFiles, false);
                appendNetworkLog("Nhận danh sách " + remoteFiles.size()
                        + " tệp từ Peer " + peersToQuery.get(0));
            }
        });

        Thread t = new Thread(listTask, "p2p-list-files-client");
        t.setDaemon(true);
        t.start();
    }

    private void addOrUpdateFileEntries(List<SharedFile> incomingFiles, boolean replaceAll) {
        if (replaceAll) {
            rawFileEntries.clear();
        }
        for (SharedFile incoming : incomingFiles) {
            rawFileEntries.removeIf(existing ->
                    existing.ownerIp().equalsIgnoreCase(incoming.ownerIp())
                            && existing.fileName().equals(incoming.fileName()));
            rawFileEntries.add(incoming);
        }
        rebuildMergedFileTable();
    }

    private void rebuildMergedFileTable() {
        Map<String, SharedFile> mergedBySha256 = new LinkedHashMap<>();
        for (SharedFile entry : rawFileEntries) {
            String key = entry.sha256Hash().toLowerCase(Locale.ROOT);
            SharedFile existing = mergedBySha256.get(key);
            if (existing == null) {
                mergedBySha256.put(key, entry);
            } else {
                mergedBySha256.put(key, existing.mergeWith(entry));
            }
        }
        masterFileList.setAll(mergedBySha256.values());
    }

    public void appendNetworkLog(String eventMessage) {
        String timestamp = LocalTime.now().format(TIME_FORMATTER);
        String line = "[" + timestamp + "] " + eventMessage + "\n";
        runOnFxThread(() -> {
            if (networkLogArea != null) {
                networkLogArea.appendText(line);
            }
        });
    }

    private void updateSharedFolderLabel() {
        if (sharedFolderLabel != null) {
            sharedFolderLabel.setText("Thư mục chia sẻ: " + sharedFolderManager.getSharedFolderPath()
                    + " (Tự động đồng bộ NIO WatchService)");
        }
    }

    private void bindUiToTask(Task<?> task) {
        transferProgressBar.progressProperty().unbind();
        statusLabel.textProperty().unbind();

        transferProgressBar.progressProperty().bind(task.progressProperty());
        statusLabel.textProperty().bind(task.messageProperty());
    }

    private Peer findPeerByEndpoint(String host, int port) {
        for (Peer p : peerList) {
            if (p.ipAddress().equalsIgnoreCase(host) && p.tcpPort() == port) {
                return p;
            }
        }
        return null;
    }

    private int indexOfPeer(Peer peer) {
        for (int i = 0; i < peerList.size(); i++) {
            if (peerList.get(i).isSamePeer(peer)) {
                return i;
            }
        }
        return -1;
    }

    private void setStatus(String message) {
        statusLabel.textProperty().unbind();
        statusLabel.setText(STATUS_PREFIX + message);
    }

    private static void runOnFxThread(Runnable action) {
        if (Platform.isFxApplicationThread()) {
            action.run();
            return;
        }
        try {
            Platform.runLater(action);
        } catch (IllegalStateException ignored) {
        }
    }
}
