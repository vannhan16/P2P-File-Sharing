package com.nhannv.p2p.ui;

import com.nhannv.p2p.MainApplication;
import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.DownloadManager;
import com.nhannv.p2p.network.FileSenderTask;
import com.nhannv.p2p.network.P2PServer;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import com.nhannv.p2p.util.FirewallUtil;
import com.nhannv.p2p.util.SharedFolderManager;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Controller trung tâm của {@code MainView.fxml}.
 *
 * <h2>Các tính năng nổi bật (Thuyết trình &amp; Vấn đáp đồ án)</h2>
 * <ul>
 *   <li><b>Hỗ trợ Radmin VPN ({@code 26.x.x.x}) &amp; Kết nối trực tiếp qua IP:</b>
 *       Hiển thị rõ cả <b>IP LAN nội bộ</b> và <b>IP Radmin VPN</b> ngay trên thanh tiêu đề.
 *       Người dùng có thể nhập trực tiếp IP Radmin của bạn cùng nhóm và bấm <b>"Kết nối"</b>
 *       để gửi gói UDP Direct Ping + kết nối TCP {@code LIST_FILES} xuyên qua mạng ảo.</li>
 *   <li><b>Cấu hình Windows Firewall 1-Click ({@code profile=any}):</b>
 *       Tự động mở cổng UDP 8888 và TCP 8889 cho cả Domain, Private và Public network (bao gồm card Radmin VPN).</li>
 *   <li><b>Gộp nguồn Swarm theo SHA-256 &amp; Pause/Resume/Cancel:</b>
 *       Tải đa nguồn song song từ nhiều Peer hoặc tạm dừng/tiếp tục từ tệp {@code .part}.</li>
 * </ul>
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @FXML
    private Label networkIpBadgeLabel;

    @FXML
    private Label sharedFolderLabel;

    @FXML
    private Button firewallConfigButton;

    @FXML
    private Button themeToggleButton;

    @FXML
    private TextField directIpTextField;

    @FXML
    private Button connectDirectIpButton;

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

    @FXML
    private TextArea networkLogArea;

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

    /** Danh sách Peer đang online trong LAN / Radmin VPN. */
    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();

    /** Danh sách tệp thô (chưa gộp) từ máy cục bộ và các Peer. */
    private final List<SharedFile> rawFileEntries = new ArrayList<>();

    /** Danh sách tệp đã gộp theo mã SHA-256 để hiển thị lên TableView. */
    private final ObservableList<SharedFile> masterFileList = FXCollections.observableArrayList();

    /** Danh sách bọc quanh masterFileList để lọc theo ô tìm kiếm tức thời. */
    private FilteredList<SharedFile> filteredFileList;

    private final SharedFolderManager sharedFolderManager = new SharedFolderManager();
    private final DownloadManager downloadManager = new DownloadManager(this::appendNetworkLog);
    private DiscoveryService discoveryService;
    private P2PServer p2pServer;

    @FXML
    private void initialize() {
        // --- 1. Cột trái: Danh sách Peer đang online ---
        peerListView.setItems(peerList);
        peerListView.setPlaceholder(new Label("Đang tìm Peer (LAN / Radmin VPN)..."));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        peerListView.getSelectionModel().selectedItemProperty().addListener((obs, oldPeer, selectedPeer) -> {
            if (selectedPeer != null) {
                fetchFilesFromPeersAsync(List.of(selectedPeer));
            }
        });

        // --- 2. Vùng giữa: Bảng 5 cột (Tên tệp, Dung lượng, Số nguồn, Máy sở hữu, Mã SHA-256) ---
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        fileSourcesColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerCountDisplay()));
        fileOwnerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().ownerDisplay()));
        fileHashColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Hash()));
        sharedFileTable.setPlaceholder(new Label("Chưa có tệp nào trong thư mục chia sẻ hoặc từ các Peer"));

        filteredFileList = new FilteredList<>(masterFileList, file -> true);
        sharedFileTable.setItems(filteredFileList);

        searchTextField.textProperty().addListener((obs, oldQuery, newQuery) -> {
            String keyword = (newQuery == null) ? "" : newQuery.trim().toLowerCase(Locale.ROOT);
            filteredFileList.setPredicate(file -> matchesSearchKeyword(file, keyword));
        });

        updateSharedFolderLabel();
        refreshHeaderIpBadges();
        appendNetworkLog("Khởi tạo hệ thống P2P File Sharing (Hỗ trợ LAN nội bộ & Radmin VPN 26.x.x.x)...");
        logDetectedPhysicalSubnets();
        checkFirewallStatusAsync();

        // --- 3. Khởi động UDP Discovery & P2P Multi-Command TCP Server ---
        startDiscovery();
        startP2PServer();

        // --- 4. Quét thư mục Shared mặc định lần đầu trên Background Thread ---
        scanLocalSharedFolderAsync();
    }

    /**
     * Cập nhật nhãn hiển thị rõ cả <b>IP LAN nội bộ</b> và <b>IP Radmin VPN</b> trên thanh tiêu đề.
     */
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
                + " (PING, LIST_FILES, DOWNLOAD_FILE, DOWNLOAD_CHUNK, PUSH_FILE).");
    }

    /** Giải phóng tài nguyên mạng khi người dùng bấm X tắt ứng dụng. */
    public void shutdown() {
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
            rawFileEntries.removeIf(f -> f.ownerIp().equals(peer.ipAddress()));
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
    // TÁC VỤ 2: Kết nối trực tiếp qua IP (Radmin VPN 26.x.x.x / LAN)
    // ------------------------------------------------------------------------------------------

    /**
     * Xử lý sự kiện khi người dùng nhập IP Radmin (vd: {@code 26.x.x.x}) vào ô {@code directIpTextField}
     * và bấm nút <b>"Kết nối"</b> (hoặc nhấn phím Enter):
     * <ol>
     *   <li>Gửi ngay một gói UDP Unicast {@code "PING"} qua {@link DiscoveryService#sendDirectPing(String)}.</li>
     *   <li>Mở kết nối TCP tới {@code ip:8889}, gửi lệnh {@code "PING"} + {@code "LIST_FILES"} để xác thực
     *       máy bạn mình đang bật ứng dụng, tự động thêm Peer vào {@code peerListView} và tải ngay
     *       danh sách tệp chia sẻ của họ lên {@code sharedFileTable}.</li>
     * </ol>
     */
    @FXML
    private void handleConnectDirectIp() {
        String rawIp = (directIpTextField == null) ? "" : directIpTextField.getText();
        String targetIp = (rawIp == null) ? "" : rawIp.trim();
        if (targetIp.isEmpty()) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa nhập địa chỉ IP");
            alert.setHeaderText("Vui lòng nhập địa chỉ IP Radmin VPN (vd: 26.x.x.x) hoặc IP LAN!");
            alert.setContentText("Bạn có thể copy IPv4 của bạn mình từ cửa sổ Radmin VPN rồi dán vào ô này.");
            alert.showAndWait();
            return;
        }

        if (connectDirectIpButton != null) {
            connectDirectIpButton.setDisable(true);
        }
        appendNetworkLog("Đang kết nối trực tiếp tới IP " + targetIp + " (UDP Direct Ping + TCP LIST_FILES)...");

        Task<List<SharedFile>> connectTask = new Task<>() {
            private Peer connectedPeer;

            @Override
            protected List<SharedFile> call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang gửi Direct Ping và kết nối TCP tới " + targetIp + ":8889...");

                // Bước 1: Bắn gói UDP Unicast PING tới thẳng targetIp:8888
                if (discoveryService != null) {
                    discoveryService.sendDirectPing(targetIp);
                }

                // Bước 2: Mở kết nối TCP tới targetIp:8889 để lấy hostname và kiểm tra máy kia đang bật app
                if (discoveryService != null) {
                    connectedPeer = discoveryService.connectAndVerifyPeerTcp(
                            targetIp,
                            P2PServer.DEFAULT_PORT,
                            3_000
                    );
                }
                if (connectedPeer == null) {
                    connectedPeer = new Peer(targetIp, "Peer-" + targetIp, System.currentTimeMillis());
                }

                // Bước 3: Gửi lệnh LIST_FILES qua TCP để lấy toàn bộ danh sách tệp chia sẻ của Peer đó
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
            // Đảm bảo Peer xuất hiện trong peerListView và được chọn luôn
            Peer peerToSelect = findPeerByIp(targetIp);
            if (peerToSelect == null) {
                String ownerName = (remoteFiles != null && !remoteFiles.isEmpty())
                        ? remoteFiles.get(0).ownerName()
                        : ("Peer-" + targetIp);
                peerToSelect = new Peer(targetIp, ownerName, System.currentTimeMillis());
                peerList.add(peerToSelect);
            }
            peerListView.getSelectionModel().select(peerToSelect);

            rawFileEntries.removeIf(f -> f.ownerIp().equals(targetIp));
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
            String msg = "Không kết nối được tới " + targetIp + ":8889 (" + reason
                    + "). Hãy kiểm tra máy bạn đã bật app và bấm 'Cấu hình Firewall (1-Click)' chưa!";
            setStatus(msg);
            appendNetworkLog("LỖI kết nối trực tiếp tới " + targetIp + ": " + msg);
        });

        bindUiToTask(connectTask);
        Thread t = new Thread(connectTask, "p2p-direct-ip-connector");
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------------------------------------
    // Các sự kiện giao diện khác
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
        appendNetworkLog("Đã đổi thư mục chia sẻ sang: " + selectedDir.getAbsolutePath());
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
            alert.setContentText("Bạn cần chọn máy đích trước khi chọn tệp để gửi.");
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

        appendNetworkLog("Bắt đầu Push tệp \"" + selectedFile.getName() + "\" tới " + selectedPeer + "...");
        FileSenderTask senderTask = new FileSenderTask(selectedFile, selectedPeer.ipAddress());
        bindUiToTask(senderTask);

        Thread senderThread = new Thread(senderTask, "p2p-tcp-file-sender");
        senderThread.setDaemon(true);
        senderThread.start();
    }

    @FXML
    private void onChooseFileClicked() {
        handleChooseFile();
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
                    rawFileEntries.removeIf(f -> f.ownerIp().equals(p.ipAddress()));
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
                    existing.ownerIp().equals(incoming.ownerIp())
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
            sharedFolderLabel.setText("Thư mục chia sẻ: " + sharedFolderManager.getSharedFolderPath());
        }
    }

    private void bindUiToTask(Task<?> task) {
        transferProgressBar.progressProperty().unbind();
        statusLabel.textProperty().unbind();

        transferProgressBar.progressProperty().bind(task.progressProperty());
        statusLabel.textProperty().bind(task.messageProperty());
    }

    private Peer findPeerByIp(String ip) {
        for (Peer p : peerList) {
            if (p.ipAddress().equals(ip)) {
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
