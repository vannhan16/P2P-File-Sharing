package com.nhannv.p2p.ui;

import com.nhannv.p2p.MainApplication;
import com.nhannv.p2p.ai.AIManager;
import com.nhannv.p2p.ai.FileContentExtractor;
import com.nhannv.p2p.ai.HeuristicOfflineAIService;
import com.nhannv.p2p.ai.NetworkDiagnosticContext;
import com.nhannv.p2p.model.Peer;
import com.nhannv.p2p.model.SharedFile;
import com.nhannv.p2p.model.TransferHistoryItem;
import com.nhannv.p2p.network.DiscoveryService;
import com.nhannv.p2p.network.DownloadManager;
import com.nhannv.p2p.network.DownloadResult;
import com.nhannv.p2p.network.FileSenderTask;
import com.nhannv.p2p.network.P2PServer;
import com.nhannv.p2p.network.PeerDiscoveryListener;
import com.nhannv.p2p.util.AppThreadPool;
import com.nhannv.p2p.util.BandwidthLimiter;
import com.nhannv.p2p.util.CryptoUtil;
import com.nhannv.p2p.util.FirewallUtil;
import com.nhannv.p2p.util.HashUtil;
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
import javafx.geometry.Pos;
import javafx.scene.chart.AreaChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.Dragboard;
import javafx.scene.input.TransferMode;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Window;
import javafx.util.Duration;
import org.kordamp.ikonli.feather.Feather;
import org.kordamp.ikonli.javafx.FontIcon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controller trung tâm của {@code MainView.fxml} — Áp dụng trọn vẹn hệ thống thiết kế
 * <b>JavaFX Crafted Desktop UI</b> (Linear / LocalSend / Fluent Design System) cùng toàn bộ
 * các tính năng từ Milestone 1 đến Milestone 6 Enterprise.
 */
public class MainController implements PeerDiscoveryListener {

    private static final String STATUS_PREFIX = "Trạng thái: ";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int SPEED_WINDOW_SECONDS = 30;

    // --- 1. Top Header Bar & Network Identity Pills ---
    @FXML
    private Label brandTitleLabel;

    @FXML
    private Label serverStatusPill;

    @FXML
    private Label lanIpPillLabel;

    @FXML
    private Label radminIpPillLabel;

    @FXML
    private Label pinStatusPillLabel;

    @FXML
    private Button aiProviderPillButton;

    @FXML
    private Button firewallConfigButton;

    @FXML
    private Button themeToggleButton;

    // --- 2. Left Sidebar: Device Radar & Security PIN / Invite Card ---
    @FXML
    private TextField directIpTextField;

    @FXML
    private Button connectDirectIpButton;

    @FXML
    private CheckBox pinEnableCheckBox;

    @FXML
    private TextField myPinTextField;

    @FXML
    private Button copyInviteTokenButton;

    @FXML
    private CheckBox askPushConfirmCheckBox;

    @FXML
    private TextField peerPinTextField;

    @FXML
    private ListView<Peer> peerListView;

    @FXML
    private Label peerCountLabel;

    @FXML
    private Button deepScanButton;

    @FXML
    private Label sharedFolderLabel;

    // --- 3. Center Workspace: Tab 1 (Files & Chunk Matrix), Tab 2 (CSV History), Tab 3 (Encrypted Chat) ---
    @FXML
    private Label searchIconHolder;

    @FXML
    private TextField searchTextField;

    @FXML
    private Button aiPreviewFileButton;

    @FXML
    private Button shareFolderZipButton;

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
    private HBox chunkMatrixContainer;

    @FXML
    private Button aiDiagnoseNetworkButton;

    @FXML
    private TextArea networkLogArea;

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
    private TextArea chatHistoryArea;

    @FXML
    private TextField chatInputField;

    @FXML
    private Button sendChatButton;

    @FXML
    private Button broadcastChatButton;

    // --- 4. Bottom Transfer Dock: Status, Speed Metric, 6px Progress, AreaChart & Controls ---
    @FXML
    private Label statusLabel;

    @FXML
    private Label liveSpeedBadgeLabel;

    @FXML
    private ProgressBar transferProgressBar;

    @FXML
    private ComboBox<String> bandwidthLimitComboBox;

    @FXML
    private AreaChart<Number, Number> speedTelemetryChart;

    @FXML
    private NumberAxis speedTimeAxis;

    @FXML
    private NumberAxis speedValueAxis;

    @FXML
    private Button downloadSelectedButton;

    @FXML
    private Button chooseFileButton;

    @FXML
    private Button pushFolderButton;

    @FXML
    private Button pauseButton;

    @FXML
    private Button resumeButton;

    @FXML
    private Button cancelButton;

    private final ObservableList<Peer> peerList = FXCollections.observableArrayList();
    private final List<SharedFile> rawFileEntries = new ArrayList<>();
    private final ObservableList<SharedFile> masterFileList = FXCollections.observableArrayList();
    private FilteredList<SharedFile> filteredFileList;
    private final ObservableList<TransferHistoryItem> historyList = FXCollections.observableArrayList();

    private final Region[] chunkMatrixCells = new Region[DownloadManager.MATRIX_CHUNK_COUNT];

    private final SharedFolderManager sharedFolderManager = new SharedFolderManager();
    private final DownloadManager downloadManager = new DownloadManager(this::appendNetworkLog);
    private DiscoveryService discoveryService;
    private P2PServer p2pServer;

    private final AtomicReference<Double> latestSpeedSampleMBps = new AtomicReference<>(0.0);
    private XYChart.Series<Number, Number> speedSeries;
    private Timeline speedChartTimeline;
    private int chartElapsedSeconds = 0;

    @FXML
    private void initialize() {
        // --- 1. Gắn Vector Icons (Ikonli Feather) sắc nét cho toàn bộ nút bấm & nhãn ---
        configureVectorIcons();

        // --- 2. Khởi tạo mã PIN bảo mật & Radar Thiết bị (Custom ListCell<Peer>) ---
        initializeSecurityPinControls();

        peerListView.setItems(peerList);
        peerListView.setPlaceholder(createCraftedPlaceholder(
                Feather.RADIO,
                "Đang dò tìm thiết bị trên LAN / Radmin...",
                "Hoặc nhập trực tiếp IP:Port / Mã mời ở ô phía trên"
        ));
        peerCountLabel.textProperty().bind(Bindings.size(peerList).asString("(%d)"));

        peerListView.getSelectionModel().selectedItemProperty().addListener((obs, oldPeer, selectedPeer) -> {
            if (selectedPeer != null) {
                fetchFilesFromPeersAsync(List.of(selectedPeer));
            }
        });

        setupPeerListCustomCardCells();

        // --- 3. Khởi tạo Bảng Tệp Chia Sẻ với Custom TableCell (SHA-256 Badge, Monospace Size) ---
        configureSharedFileTableColumns();
        sharedFileTable.setPlaceholder(createCraftedPlaceholder(
                Feather.INBOX,
                "Chưa có tệp nào trong không gian chia sẻ",
                "Kéo thả Tệp hoặc cả Thư mục từ Windows Explorer vào bảng này để chia sẻ ngay"
        ));

        filteredFileList = new FilteredList<>(masterFileList, file -> true);
        sharedFileTable.setItems(filteredFileList);

        searchTextField.textProperty().addListener((obs, oldQuery, newQuery) -> {
            String keyword = (newQuery == null) ? "" : newQuery.trim().toLowerCase(Locale.ROOT);
            filteredFileList.setPredicate(file -> matchesSearchKeyword(file, keyword));
        });

        setupSharedTableDragAndDrop();

        // --- 4. Khởi tạo Bản đồ 32 phân đoạn (BitTorrent Chunk Matrix) & Đồ thị tốc độ Dock ---
        initializeChunkMatrixBar();
        initializeSpeedTelemetryChart();

        // --- 5. Khởi tạo Bảng Lịch sử truyền tải & Bộ giới hạn băng thông QoS ---
        initializeHistoryTable();
        initializeBandwidthLimiterComboBox();

        downloadManager.setAuthPinSupplier(this::getPeerAuthPinInput);
        downloadManager.setSpeedSampleCallback(this::recordSpeedSample);
        downloadManager.setHistoryCallback(this::recordTransferHistory);
        downloadManager.setChunkMatrixCallback(this::updateChunkMatrixCell);

        updateSharedFolderLabel();
        refreshHeaderIpBadges();
        refreshAiProviderPill();
        appendNetworkLog("Khởi tạo P2P File Sharing — Crafted Desktop UI & AI-Augmented P2P ("
                + AIManager.getInstance().getActiveProviderBadgeText() + ")...");
        appendChatLog("Hệ thống", "Kênh Chat P2P bảo mật đầu-cuối (AES-256-GCM) đã sẵn sàng.");
        logDetectedPhysicalSubnets();
        checkFirewallStatusAsync();

        // --- 6. Khởi động dịch vụ mạng & NIO WatchService ---
        startDiscovery();
        startP2PServer();
        scanLocalSharedFolderAsync();
        startSharedFolderRealTimeWatcher();
    }

    // ------------------------------------------------------------------------------------------
    // CẤU HÌNH VECTOR ICONS (IKONLI FEATHER) & CUSTOM CELLS CHUẨN CRAFTED UI
    // ------------------------------------------------------------------------------------------

    private static FontIcon icon(Feather featherIcon, int size, String hexColor) {
        FontIcon fi = new FontIcon(featherIcon);
        fi.setIconSize(size);
        if (hexColor != null && !hexColor.isBlank()) {
            fi.setIconColor(Color.web(hexColor));
        }
        return fi;
    }

    private void configureVectorIcons() {
        if (brandTitleLabel != null) {
            brandTitleLabel.setGraphic(icon(Feather.SHARE_2, 16, "#3B82F6"));
            brandTitleLabel.setGraphicTextGap(8);
        }
        if (serverStatusPill != null) {
            serverStatusPill.setGraphic(icon(Feather.ACTIVITY, 12, "#10B981"));
            serverStatusPill.setGraphicTextGap(5);
        }
        if (lanIpPillLabel != null) {
            lanIpPillLabel.setGraphic(icon(Feather.WIFI, 12, "#3B82F6"));
            lanIpPillLabel.setGraphicTextGap(5);
        }
        if (radminIpPillLabel != null) {
            radminIpPillLabel.setGraphic(icon(Feather.SHIELD, 12, "#8B949E"));
            radminIpPillLabel.setGraphicTextGap(5);
        }
        if (pinStatusPillLabel != null) {
            pinStatusPillLabel.setGraphic(icon(Feather.LOCK, 12, "#8B949E"));
            pinStatusPillLabel.setGraphicTextGap(5);
        }
        if (aiProviderPillButton != null) {
            aiProviderPillButton.setGraphic(icon(Feather.CPU, 12, "#3B82F6"));
            aiProviderPillButton.setGraphicTextGap(5);
        }
        if (firewallConfigButton != null) {
            firewallConfigButton.setGraphic(icon(Feather.SHIELD, 13, "#F59E0B"));
        }
        if (themeToggleButton != null) {
            updateThemeButtonGraphic(MainApplication.isDarkThemeActive());
        }
        if (connectDirectIpButton != null) {
            connectDirectIpButton.setGraphic(icon(Feather.LINK, 13, "#FFFFFF"));
        }
        if (copyInviteTokenButton != null) {
            copyInviteTokenButton.setGraphic(icon(Feather.COPY, 12, "#3B82F6"));
        }
        if (deepScanButton != null) {
            deepScanButton.setGraphic(icon(Feather.RADIO, 12, "#3B82F6"));
        }
        if (searchIconHolder != null) {
            searchIconHolder.setGraphic(icon(Feather.SEARCH, 14, "#8B949E"));
        }
        if (aiPreviewFileButton != null) {
            aiPreviewFileButton.setGraphic(icon(Feather.ZAP, 13, "#3B82F6"));
        }
        if (shareFolderZipButton != null) {
            shareFolderZipButton.setGraphic(icon(Feather.ARCHIVE, 13, "#3B82F6"));
        }
        if (chooseSharedFolderButton != null) {
            chooseSharedFolderButton.setGraphic(icon(Feather.FOLDER, 13, "#8B949E"));
        }
        if (refreshFilesButton != null) {
            refreshFilesButton.setGraphic(icon(Feather.REFRESH_CW, 13, "#8B949E"));
        }
        if (aiDiagnoseNetworkButton != null) {
            aiDiagnoseNetworkButton.setGraphic(icon(Feather.ACTIVITY, 13, "#3B82F6"));
        }
        if (exportCsvButton != null) {
            exportCsvButton.setGraphic(icon(Feather.DOWNLOAD, 13, "#FFFFFF"));
        }
        if (sendChatButton != null) {
            sendChatButton.setGraphic(icon(Feather.SEND, 13, "#FFFFFF"));
        }
        if (broadcastChatButton != null) {
            broadcastChatButton.setGraphic(icon(Feather.RADIO, 13, "#3B82F6"));
        }
        if (downloadSelectedButton != null) {
            downloadSelectedButton.setGraphic(icon(Feather.DOWNLOAD_CLOUD, 14, "#FFFFFF"));
        }
        if (chooseFileButton != null) {
            chooseFileButton.setGraphic(icon(Feather.UPLOAD, 13, "#3B82F6"));
        }
        if (pushFolderButton != null) {
            pushFolderButton.setGraphic(icon(Feather.FOLDER_PLUS, 13, "#3B82F6"));
        }
        if (pauseButton != null) {
            pauseButton.setGraphic(icon(Feather.PAUSE, 13, "#F59E0B"));
        }
        if (resumeButton != null) {
            resumeButton.setGraphic(icon(Feather.PLAY, 13, "#10B981"));
        }
        if (cancelButton != null) {
            cancelButton.setGraphic(icon(Feather.X_CIRCLE, 13, "#EF4444"));
        }
    }

    private void updateThemeButtonGraphic(boolean isDark) {
        if (themeToggleButton == null) {
            return;
        }
        if (isDark) {
            themeToggleButton.setText("Light Mode");
            themeToggleButton.setGraphic(icon(Feather.SUN, 13, "#F59E0B"));
        } else {
            themeToggleButton.setText("Dark Mode");
            themeToggleButton.setGraphic(icon(Feather.MOON, 13, "#3B82F6"));
        }
    }

    private static VBox createCraftedPlaceholder(Feather featherIcon, String title, String subtitle) {
        VBox box = new VBox(6);
        box.getStyleClass().add("empty-placeholder-box");
        box.setAlignment(Pos.CENTER);

        FontIcon iconNode = icon(featherIcon, 28, "#6E7681");
        Label titleLabel = new Label(title);
        titleLabel.getStyleClass().add("empty-placeholder-title");

        Label subLabel = new Label(subtitle);
        subLabel.getStyleClass().add("empty-placeholder-subtitle");

        box.getChildren().addAll(iconNode, titleLabel, subLabel);
        return box;
    }

    /**
     * Custom {@link ListCell} cho danh sách Peer ở Sidebar bên trái (Tuyệt đối không dùng {@code toString()}):
     * Mỗi Peer là một Card gồm chấm tròn xanh lá ({@code Circle} 8px), Tên máy in đậm (13px),
     * icon khóa nếu bật PIN, badge độ trễ RTT {@code ms} và địa chỉ IP:Port font Monospace (11px) ở dòng dưới.
     */
    private void setupPeerListCustomCardCells() {
        peerListView.setCellFactory(listView -> {
            ListCell<Peer> cell = new ListCell<>() {
                private final VBox cardRoot = new VBox(3);
                private final HBox topRow = new HBox(6);
                private final Circle onlineDot = new Circle(4.0);
                private final Label hostNameLabel = new Label();
                private final Region spacer = new Region();
                private final Label rttBadgeLabel = new Label();
                private final Label ipEndpointLabel = new Label();

                {
                    cardRoot.getStyleClass().add("peer-card-item");
                    topRow.setAlignment(Pos.CENTER_LEFT);
                    onlineDot.getStyleClass().add("peer-online-dot");
                    hostNameLabel.getStyleClass().add("peer-card-hostname");
                    HBox.setHgrow(spacer, Priority.ALWAYS);
                    rttBadgeLabel.getStyleClass().add("peer-rtt-badge");
                    ipEndpointLabel.getStyleClass().add("peer-card-ip");

                    topRow.getChildren().addAll(onlineDot, hostNameLabel, spacer, rttBadgeLabel);
                    cardRoot.getChildren().addAll(topRow, ipEndpointLabel);
                }

                @Override
                protected void updateItem(Peer peer, boolean empty) {
                    super.updateItem(peer, empty);
                    if (empty || peer == null) {
                        setText(null);
                        setGraphic(null);
                    } else {
                        hostNameLabel.setText(peer.hostName());
                        if (peer.pinProtected()) {
                            hostNameLabel.setGraphic(icon(Feather.LOCK, 11, "#F59E0B"));
                            hostNameLabel.setGraphicTextGap(4);
                        } else {
                            hostNameLabel.setGraphic(null);
                        }

                        if (peer.latencyMs() >= 0) {
                            rttBadgeLabel.setText(peer.latencyMs() + " ms");
                            rttBadgeLabel.setVisible(true);
                            rttBadgeLabel.setManaged(true);
                        } else {
                            rttBadgeLabel.setVisible(false);
                            rttBadgeLabel.setManaged(false);
                        }

                        ipEndpointLabel.setText(peer.endpoint());
                        setText(null);
                        setGraphic(cardRoot);
                    }
                }
            };

            cell.setOnDragOver(event -> {
                if (!cell.isEmpty() && cell.getItem() != null && event.getDragboard().hasFiles()) {
                    event.acceptTransferModes(TransferMode.COPY);
                    if (cell.getGraphic() != null && !cell.getGraphic().getStyleClass().contains("peer-card-drop-target")) {
                        cell.getGraphic().getStyleClass().add("peer-card-drop-target");
                    }
                }
                event.consume();
            });

            cell.setOnDragExited(event -> {
                if (cell.getGraphic() != null) {
                    cell.getGraphic().getStyleClass().remove("peer-card-drop-target");
                }
                event.consume();
            });

            cell.setOnDragDropped(event -> {
                if (cell.getGraphic() != null) {
                    cell.getGraphic().getStyleClass().remove("peer-card-drop-target");
                }
                Peer targetPeer = cell.getItem();
                Dragboard db = event.getDragboard();
                boolean success = false;
                if (targetPeer != null && db.hasFiles()) {
                    for (File item : db.getFiles()) {
                        if (item.isDirectory()) {
                            packageAndPushDirectoryToPeer(item, targetPeer);
                            success = true;
                        } else if (item.isFile()) {
                            pushFileToPeer(item, targetPeer);
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
     * Cấu hình các cột của {@code sharedFileTable} với Custom {@link TableCell}:
     * <ul>
     *   <li>Cột Tên tệp: Kèm Vector Icon phân biệt tệp nén {@code .zip} và tệp thường.</li>
     *   <li>Cột Dung lượng: Căn phải ({@code CENTER_RIGHT}) và dùng font Monospace.</li>
     *   <li>Cột Nguồn tải: Hiển thị dạng Semantic Pill ({@code Swarm N Peers} hoặc {@code 1 Peer}).</li>
     *   <li>Cột Thiết bị sở hữu: Hiển thị font Monospace cho IP/Endpoint.</li>
     *   <li>Cột SHA-256: Hiển thị dạng thẻ code rút gọn {@code a8f5...92c1} kèm Tooltip full 64 ký tự và click để copy.</li>
     * </ul>
     */
    private void configureSharedFileTableColumns() {
        fileNameColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));
        fileNameColumn.setCellFactory(col -> new TableCell<>() {
            private final HBox cellBox = new HBox(7);
            private final Label nameLabel = new Label();
            private final Label tagBadge = new Label();
            private final Label warningBadge = new Label();
            private final Tooltip aiTooltip = new Tooltip();

            {
                cellBox.setAlignment(Pos.CENTER_LEFT);
                tagBadge.getStyleClass().addAll("network-pill", "pill-neutral");
                warningBadge.getStyleClass().addAll("network-pill", "pill-warning");
                warningBadge.setText("Sensitive");
                cellBox.getChildren().addAll(nameLabel, warningBadge, tagBadge);
            }

            @Override
            protected void updateItem(String name, boolean empty) {
                super.updateItem(name, empty);
                if (empty || name == null || getTableRow() == null || getTableRow().getItem() == null) {
                    setText(null);
                    setGraphic(null);
                    setTooltip(null);
                } else {
                    SharedFile sf = getTableRow().getItem();
                    boolean isArchive = name.toLowerCase(Locale.ROOT).endsWith(".zip");
                    nameLabel.setGraphic(icon(isArchive ? Feather.ARCHIVE : Feather.FILE_TEXT, 14,
                            isArchive ? "#F59E0B" : "#3B82F6"));
                    nameLabel.setGraphicTextGap(8);
                    nameLabel.setText(name);

                    if (sf.hasSensitiveWarning()) {
                        warningBadge.setGraphic(icon(Feather.ALERT_TRIANGLE, 11, "#F59E0B"));
                        warningBadge.setGraphicTextGap(4);
                        warningBadge.setVisible(true);
                        warningBadge.setManaged(true);
                    } else {
                        warningBadge.setVisible(false);
                        warningBadge.setManaged(false);
                    }

                    String primaryTag = sf.primaryTagBadge();
                    if (!primaryTag.isBlank()) {
                        tagBadge.setText(primaryTag);
                        tagBadge.setVisible(true);
                        tagBadge.setManaged(true);
                    } else {
                        tagBadge.setVisible(false);
                        tagBadge.setManaged(false);
                    }

                    if (sf.hasAiMetadata() || sf.hasSensitiveWarning()) {
                        StringBuilder tip = new StringBuilder();
                        tip.append("Tệp: ").append(sf.fileName()).append(" (").append(sf.formattedSize()).append(")\n");
                        if (!sf.aiTags().isBlank()) {
                            tip.append("Thẻ ngữ nghĩa (AI Tags): ").append(sf.aiTags()).append("\n");
                        }
                        if (!sf.aiSummary().isBlank()) {
                            tip.append("Tóm tắt AI: ").append(sf.aiSummary());
                        }
                        if (sf.hasSensitiveWarning()) {
                            tip.append("\n[!] ").append(sf.sensitiveWarning());
                        }
                        aiTooltip.setText(tip.toString().trim());
                        setTooltip(aiTooltip);
                    } else {
                        setTooltip(null);
                    }

                    setText(null);
                    setGraphic(cellBox);
                }
            }
        });

        fileSizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        fileSizeColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("cell-right-mono");
            }

            @Override
            protected void updateItem(String sizeText, boolean empty) {
                super.updateItem(sizeText, empty);
                setText(empty ? null : sizeText);
            }
        });

        fileSourcesColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerCountDisplay()));
        fileSourcesColumn.setCellFactory(col -> new TableCell<>() {
            private final Label badge = new Label();

            @Override
            protected void updateItem(String val, boolean empty) {
                super.updateItem(val, empty);
                if (empty || val == null || getTableRow() == null || getTableRow().getItem() == null) {
                    setGraphic(null);
                    setText(null);
                } else {
                    SharedFile sf = getTableRow().getItem();
                    badge.setText(val);
                    badge.getStyleClass().setAll("network-pill",
                            sf.peerCount() > 1 ? "pill-sync" : "pill-neutral");
                    setGraphic(badge);
                    setText(null);
                }
            }
        });

        fileOwnerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().ownerDisplay()));
        fileOwnerColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("mono-text");
            }

            @Override
            protected void updateItem(String owner, boolean empty) {
                super.updateItem(owner, empty);
                setText(empty ? null : owner);
            }
        });

        fileHashColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Hash()));
        fileHashColumn.setCellFactory(col -> new TableCell<>() {
            private final Label hashBadge = new Label();
            private final Tooltip fullHashTooltip = new Tooltip();

            {
                hashBadge.getStyleClass().add("sha-code-badge");
                hashBadge.setOnMouseClicked(e -> {
                    if (getItem() != null && !getItem().isBlank()) {
                        ClipboardContent cc = new ClipboardContent();
                        cc.putString(getItem());
                        Clipboard.getSystemClipboard().setContent(cc);
                        setStatus("Đã copy mã SHA-256 đầy đủ (64 ký tự) vào Clipboard: " + HashUtil.shortHash(getItem()));
                    }
                });
            }

            @Override
            protected void updateItem(String fullHash, boolean empty) {
                super.updateItem(fullHash, empty);
                if (empty || fullHash == null || fullHash.isBlank()) {
                    setGraphic(null);
                    setText(null);
                } else {
                    hashBadge.setText(HashUtil.shortHash(fullHash));
                    hashBadge.setGraphic(icon(Feather.COPY, 11, "#3B82F6"));
                    hashBadge.setGraphicTextGap(5);
                    fullHashTooltip.setText("SHA-256 đầy đủ (Click để copy):\n" + fullHash);
                    Tooltip.install(hashBadge, fullHashTooltip);
                    setGraphic(hashBadge);
                    setText(null);
                }
            }
        });
    }

    // ------------------------------------------------------------------------------------------
    // BẢO MẬT PIN, HỘP THOẠI PHÊ DUYỆT PUSH & MÃ MỜI 1-CLICK (host:port#PIN)
    // ------------------------------------------------------------------------------------------

    private boolean isPinMasked = true;

    private void initializeSecurityPinControls() {
        String securePin = CryptoUtil.generateSecurePin6();
        if (myPinTextField != null) {
            myPinTextField.setText(securePin);
        }
        if (pinEnableCheckBox != null) {
            pinEnableCheckBox.setSelected(true);
        }
        if (askPushConfirmCheckBox != null) {
            askPushConfirmCheckBox.setSelected(true);
        }
        if (pinStatusPillLabel != null) {
            pinStatusPillLabel.setOnMouseClicked(e -> {
                isPinMasked = !isPinMasked;
                boolean enabled = pinEnableCheckBox != null && pinEnableCheckBox.isSelected();
                String pin = (myPinTextField != null) ? myPinTextField.getText() : "";
                updatePinHeaderPill(enabled, pin);
            });
            pinStatusPillLabel.setTooltip(new Tooltip("Nhấp để ẩn/hiện mã PIN (Chế độ chống lộ khi chia sẻ màn hình)"));
        }
        updatePinHeaderPill(true, securePin);
    }

    private void updatePinHeaderPill(boolean enabled, String pin) {
        if (pinStatusPillLabel == null) {
            return;
        }
        if (enabled && pin != null && !pin.isBlank()) {
            String displayPin = isPinMasked ? "******" : pin;
            pinStatusPillLabel.setText("PIN: " + displayPin);
            pinStatusPillLabel.getStyleClass().setAll("network-pill", "pill-warning");
            pinStatusPillLabel.setGraphic(icon(Feather.LOCK, 12, "#F59E0B"));
        } else {
            pinStatusPillLabel.setText("PIN: OFF");
            pinStatusPillLabel.getStyleClass().setAll("network-pill", "pill-neutral");
            pinStatusPillLabel.setGraphic(icon(Feather.UNLOCK, 12, "#8B949E"));
        }
    }

    @FXML
    private void handlePinConfigChanged() {
        boolean enabled = pinEnableCheckBox != null && pinEnableCheckBox.isSelected();
        String pin = (myPinTextField == null || myPinTextField.getText() == null)
                ? ""
                : myPinTextField.getText().trim();

        if (p2pServer != null) {
            p2pServer.updateSecurityPinConfig(enabled, pin);
        }
        updatePinHeaderPill(enabled, pin);

        if (enabled) {
            appendNetworkLog("[BẢO MẬT PIN] Đã BẬT bảo vệ bằng mã PIN (******). Mọi yêu cầu tải/gửi tệp/chat tới máy bạn phải kèm đúng mã PIN này.");
            setStatus("Đã bật mã PIN bảo vệ (******).");
        } else {
            appendNetworkLog("[BẢO MẬT PIN] Đã TẮT mã PIN bảo vệ — cho phép truyền tệp tự do.");
            setStatus("Đã tắt mã PIN bảo vệ.");
        }
    }

    @FXML
    private void handlePushConfirmConfigChanged() {
        boolean askConfirm = askPushConfirmCheckBox != null && askPushConfirmCheckBox.isSelected();
        if (p2pServer != null) {
            p2pServer.setAskBeforeAcceptingPush(askConfirm);
        }
        String msg = askConfirm
                ? "Đã BẬT chế độ hỏi xác nhận (Accept/Reject) trước khi nhận tệp Push từ Peer khác."
                : "Đã TẮT chế độ hỏi xác nhận — tự động lưu tệp Push hợp lệ vào Downloads/.";
        appendNetworkLog(msg);
        setStatus(msg);
    }

    private Boolean promptUserForPushApproval(String senderAndFile, Long fileSizeBytes) {
        CompletableFuture<Boolean> decisionFuture = new CompletableFuture<>();
        runOnFxThread(() -> {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Yêu cầu gửi tệp trực tiếp (Incoming Push Request)");

            boolean dangerous = com.nhannv.p2p.util.PathUtil.isPotentiallyDangerousFile(senderAndFile);
            if (dangerous) {
                alert.setAlertType(Alert.AlertType.WARNING);
                alert.setHeaderText("CẢNH BÁO NGUY HIỂM: Tệp thực thi/kịch bản có nguy cơ độc hại!");
                alert.setContentText("CẢNH BÁO AN NINH: Peer từ xa muốn gửi tệp thực thi: " + senderAndFile
                        + "\nDung lượng: " + SharedFile.formatSize(fileSizeBytes == null ? 0L : fileSizeBytes)
                        + "\n\nCác tệp có đuôi mở rộng này có thể chứa mã độc thực thi trực tiếp trên máy của bạn."
                        + "\nBạn có THẬT SỰ tin tưởng người gửi và đồng ý nhận tệp này vào Downloads/ không?");
            } else {
                alert.setHeaderText("Một thiết bị Peer muốn gửi tệp vào máy của bạn!");
                alert.setContentText("Thông tin tệp: " + senderAndFile
                        + "\nDung lượng: " + SharedFile.formatSize(fileSizeBytes == null ? 0L : fileSizeBytes)
                        + "\n\nBạn có đồng ý nhận tệp này vào thư mục Downloads/ không?");
            }
            ButtonType acceptBtn = new ButtonType("Đồng ý nhận (Accept)");
            ButtonType rejectBtn = new ButtonType("Từ chối (Reject)");
            alert.getButtonTypes().setAll(acceptBtn, rejectBtn);

            Optional<ButtonType> result = alert.showAndWait();
            decisionFuture.complete(result.isPresent() && result.get() == acceptBtn);
        });

        try {
            return decisionFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    @FXML
    private void handleCopyInviteToken() {
        String radminIp = DiscoveryService.detectRadminVpnIp();
        String lanIp = DiscoveryService.detectLocalLanIp();
        String bestHost = (radminIp != null && radminIp.startsWith("26.")) ? radminIp : lanIp;
        String myPin = (myPinTextField != null && myPinTextField.getText() != null)
                ? myPinTextField.getText().trim()
                : "";
        boolean pinOn = pinEnableCheckBox != null && pinEnableCheckBox.isSelected() && !myPin.isEmpty();

        String token = bestHost + ":" + P2PServer.DEFAULT_PORT + (pinOn ? ("#" + myPin) : "");

        ClipboardContent content = new ClipboardContent();
        content.putString(token);
        Clipboard.getSystemClipboard().setContent(content);

        String msg = "Đã copy Mã Mời 1-Click vào Clipboard: " + token;
        appendNetworkLog(msg);
        setStatus(msg);
    }

    private String getPeerAuthPinInput() {
        if (peerPinTextField == null || peerPinTextField.getText() == null) {
            return "";
        }
        return peerPinTextField.getText().trim();
    }

    @FXML
    private void handleConnectDirectIp() {
        String rawInput = (directIpTextField == null) ? "" : directIpTextField.getText();
        String trimmedInput = (rawInput == null) ? "" : rawInput.trim();
        if (trimmedInput.isEmpty()) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa nhập địa chỉ kết nối");
            alert.setHeaderText("Vui lòng nhập IP, host:port hoặc Mã mời (host:port#PIN)!");
            alert.setContentText("Ví dụ:\n• Mã mời 1-Click: 26.45.12.88:8889#4821\n• Internet TCP Tunnel: 0.tcp.ap.ngrok.io:14523");
            alert.showAndWait();
            return;
        }

        Peer.InviteToken invite = Peer.parseInviteToken(trimmedInput, P2PServer.DEFAULT_PORT);
        if (!invite.pin().isEmpty() && peerPinTextField != null) {
            peerPinTextField.setText(invite.pin());
            appendNetworkLog("Đã tự động nhận diện mã PIN đối tác (******) từ Mã Mời!");
        }

        Peer.HostPort target = invite.toHostPort();
        String targetEndpoint = target.toEndpointString();

        if (connectDirectIpButton != null) {
            connectDirectIpButton.setDisable(true);
        }
        appendNetworkLog("Đang kết nối trực tiếp tới " + targetEndpoint + " (Đo độ trễ RTT & truy vấn LIST_FILES)...");

        Task<List<SharedFile>> connectTask = new Task<>() {
            private Peer connectedPeer;

            @Override
            protected List<SharedFile> call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang kết nối TCP tới " + targetEndpoint + "...");

                if (discoveryService != null && target.port() == P2PServer.DEFAULT_PORT) {
                    discoveryService.sendDirectPing(target.host());
                }

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
                            false,
                            -1L
                    );
                }

                List<SharedFile> files = P2PServer.requestFileListFromPeer(connectedPeer, getPeerAuthPinInput());

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
                        false,
                        1L
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
        AppThreadPool.execute(connectTask);
    }

    // ------------------------------------------------------------------------------------------
    // BẢN ĐỒ 32 MẢNH TẢI THỜI GIAN THỰC (BITTORRENT CHUNK MATRIX UI - PURE CSS CLASSES)
    // ------------------------------------------------------------------------------------------

    private void initializeChunkMatrixBar() {
        if (chunkMatrixContainer == null) {
            return;
        }
        chunkMatrixContainer.getChildren().clear();
        for (int i = 0; i < DownloadManager.MATRIX_CHUNK_COUNT; i++) {
            Region cell = new Region();
            cell.setPrefHeight(12);
            cell.setMinHeight(12);
            cell.setPrefWidth(18);
            HBox.setHgrow(cell, Priority.ALWAYS);
            applyChunkCellStyleClass(cell, DownloadManager.CHUNK_STATE_WAITING);
            chunkMatrixCells[i] = cell;
            chunkMatrixContainer.getChildren().add(cell);
        }
    }

    private void updateChunkMatrixCell(int chunkIndex, int state) {
        if (chunkIndex < 0 || chunkIndex >= chunkMatrixCells.length) {
            return;
        }
        runOnFxThread(() -> {
            Region cell = chunkMatrixCells[chunkIndex];
            if (cell != null) {
                applyChunkCellStyleClass(cell, state);
            }
        });
    }

    private static void applyChunkCellStyleClass(Region cell, int state) {
        String stateClass = switch (state) {
            case DownloadManager.CHUNK_STATE_ACTIVE_PRIMARY -> "chunk-active-primary";
            case DownloadManager.CHUNK_STATE_ACTIVE_SWARM -> "chunk-active-swarm";
            case DownloadManager.CHUNK_STATE_COMPLETED -> "chunk-completed";
            case DownloadManager.CHUNK_STATE_PAUSED_OR_RETRY -> "chunk-paused-error";
            default -> "chunk-waiting";
        };
        cell.getStyleClass().setAll("chunk-cell", stateClass);
    }

    // ------------------------------------------------------------------------------------------
    // CHIA SẺ & GỬI CẢ THƯ MỤC (FOLDER ZIP STREAMING) + KÉO THẢ
    // ------------------------------------------------------------------------------------------

    @FXML
    private void handleShareDirectoryAsZip() {
        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Chọn Thư mục để đóng gói ZIP và chia sẻ ngay");
        Window owner = (shareFolderZipButton != null && shareFolderZipButton.getScene() != null)
                ? shareFolderZipButton.getScene().getWindow()
                : null;
        File selectedDir = chooser.showDialog(owner);
        if (selectedDir == null) {
            return;
        }
        handleFilesDroppedIntoSharedTable(List.of(selectedDir));
    }

    @FXML
    private void handlePushDirectory() {
        Peer selectedPeer = peerListView.getSelectionModel().getSelectedItem();
        if (selectedPeer == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn máy nhận (Peer)");
            alert.setHeaderText("Vui lòng chọn một Peer đang online ở danh sách bên trái!");
            alert.setContentText("Bạn cũng có thể kéo thả trực tiếp cả Thư mục vào thẻ Peer ở cột bên trái.");
            alert.showAndWait();
            return;
        }

        DirectoryChooser chooser = new DirectoryChooser();
        chooser.setTitle("Chọn Thư mục để đóng gói ZIP và Push tới " + selectedPeer.hostName());
        Window owner = (pushFolderButton != null && pushFolderButton.getScene() != null)
                ? pushFolderButton.getScene().getWindow()
                : null;
        File selectedDir = chooser.showDialog(owner);
        if (selectedDir == null) {
            return;
        }

        packageAndPushDirectoryToPeer(selectedDir, selectedPeer);
    }

    private void packageAndPushDirectoryToPeer(File directory, Peer targetPeer) {
        appendNetworkLog("Đang đóng gói ZIP thư mục \"" + directory.getName()
                + "\" (bộ đệm 64KB) để Push tới " + targetPeer.endpoint() + "...");

        Task<File> zipTask = new Task<>() {
            @Override
            protected File call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang nén thư mục \"" + directory.getName() + ".zip\"...");
                return sharedFolderManager.packageDirectoryToSharedZip(directory);
            }
        };

        zipTask.setOnSucceeded(e -> {
            File zipFile = zipTask.getValue();
            if (zipFile != null && zipFile.exists()) {
                appendNetworkLog("Đã đóng gói xong \"" + zipFile.getName()
                        + "\" (" + SharedFile.formatSize(zipFile.length()) + ") -> Bắt đầu Push tới " + targetPeer.endpoint());
                pushFileToPeer(zipFile, targetPeer);
            }
        });

        zipTask.setOnFailed(e -> {
            Throwable ex = zipTask.getException();
            String err = (ex != null) ? ex.getMessage() : "Lỗi nén ZIP";
            appendNetworkLog("Lỗi khi đóng gói thư mục: " + err);
            setStatus("Lỗi nén thư mục: " + err);
        });

        bindUiToTask(zipTask);
        Thread t = new Thread(zipTask, "p2p-folder-zip-pusher");
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------------------------------------
    // BỘ GIỚI HẠN BĂNG THÔNG CHỦ ĐỘNG (TOKEN BUCKET QoS)
    // ------------------------------------------------------------------------------------------

    private void initializeBandwidthLimiterComboBox() {
        if (bandwidthLimitComboBox == null) {
            return;
        }
        bandwidthLimitComboBox.setItems(FXCollections.observableArrayList(
                "Không giới hạn",
                "20 MB/s",
                "10 MB/s",
                "5 MB/s",
                "2 MB/s",
                "512 KB/s"
        ));
        bandwidthLimitComboBox.getSelectionModel().selectFirst();
    }

    @FXML
    private void handleBandwidthLimitChanged() {
        if (bandwidthLimitComboBox == null) {
            return;
        }
        String selected = bandwidthLimitComboBox.getSelectionModel().getSelectedItem();
        if (selected == null) {
            return;
        }
        long bytesPerSec = switch (selected) {
            case "20 MB/s" -> 20L * 1024 * 1024;
            case "10 MB/s" -> 10L * 1024 * 1024;
            case "5 MB/s" -> 5L * 1024 * 1024;
            case "2 MB/s" -> 2L * 1024 * 1024;
            case "512 KB/s" -> 512L * 1024;
            default -> 0L;
        };
        BandwidthLimiter.getInstance().setMaxBytesPerSecond(bytesPerSec);
        String msg = (bytesPerSec <= 0L)
                ? "[QoS Bandwidth] Tốc độ truyền tải: Không giới hạn."
                : ("[QoS Bandwidth] Giới hạn băng thông tối đa: " + selected + " (Token Bucket).");
        appendNetworkLog(msg);
        setStatus(msg);
    }

    // ------------------------------------------------------------------------------------------
    // KÊNH CHAT P2P TỨC THỜI MÃ HÓA ĐẦU-CUỐI AES-256-GCM
    // ------------------------------------------------------------------------------------------

    @FXML
    private void handleSendChatMessage() {
        String text = (chatInputField == null || chatInputField.getText() == null)
                ? ""
                : chatInputField.getText().trim();
        if (text.isEmpty()) {
            return;
        }

        Peer selectedPeer = peerListView.getSelectionModel().getSelectedItem();
        if (selectedPeer == null) {
            if (!peerList.isEmpty()) {
                selectedPeer = peerList.get(0);
            } else {
                Alert alert = new Alert(Alert.AlertType.WARNING);
                alert.setTitle("Chưa chọn Peer để gửi tin nhắn");
                alert.setHeaderText("Vui lòng chọn một thiết bị ở Radar bên trái hoặc kết nối tới một Peer trước!");
                alert.showAndWait();
                return;
            }
        }

        chatInputField.clear();
        sendEncryptedChatAsync(List.of(selectedPeer), text);
    }

    @FXML
    private void handleBroadcastChatMessage() {
        String text = (chatInputField == null || chatInputField.getText() == null)
                ? ""
                : chatInputField.getText().trim();
        if (text.isEmpty()) {
            return;
        }
        if (peerList.isEmpty()) {
            setStatus("Chưa có Peer nào đang online để gửi tin nhắn Broadcast.");
            return;
        }
        chatInputField.clear();
        sendEncryptedChatAsync(new ArrayList<>(peerList), text);
    }

    private void sendEncryptedChatAsync(List<Peer> recipients, String plainMessage) {
        String myName = resolveLocalComputerName();
        String pin = getPeerAuthPinInput();
        appendChatLog("Bạn -> " + (recipients.size() == 1 ? recipients.get(0).hostName() : ("Tất cả (" + recipients.size() + " Peers)")),
                plainMessage);

        Thread t = new Thread(() -> {
            for (Peer peer : recipients) {
                try {
                    P2PServer.sendChatMessageToPeer(peer, myName, plainMessage, pin);
                } catch (Exception e) {
                    appendNetworkLog("Không gửi được tin nhắn Chat tới " + peer.endpoint() + ": " + e.getMessage());
                }
            }
        }, "p2p-encrypted-chat-sender");
        t.setDaemon(true);
        t.start();
    }

    private void appendChatLog(String sender, String message) {
        String time = LocalTime.now().format(TIME_FORMATTER);
        String formatted = "[" + time + "] " + sender + ": " + message + "\n";
        runOnFxThread(() -> {
            if (chatHistoryArea != null) {
                chatHistoryArea.appendText(formatted);
            }
        });
    }

    private static String resolveLocalComputerName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            String env = System.getenv("COMPUTERNAME");
            return (env != null && !env.isBlank()) ? env : "My-PC";
        }
    }

    // ------------------------------------------------------------------------------------------
    // TỰ ĐỘNG ĐỒNG BỘ THƯ MỤC CHIA SẺ (JAVA NIO WATCHSERVICE) & DRAG-AND-DROP
    // ------------------------------------------------------------------------------------------

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
        appendNetworkLog("Kéo thả (Drag & Drop): Đang xử lý " + droppedFiles.size()
                + " mục vào " + sharedFolderManager.getSharedFolderPath() + "...");

        Task<List<SharedFile>> copyTask = new Task<>() {
            @Override
            protected List<SharedFile> call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang sao chép / đóng gói ZIP và băm SHA-256 cho "
                        + droppedFiles.size() + " mục...");
                int copiedCount = sharedFolderManager.copyDroppedFilesToSharedFolder(droppedFiles);
                List<SharedFile> updatedLocal = sharedFolderManager.scanAndHashSync();
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã thêm " + copiedCount
                        + " mục vào thư mục chia sẻ!");
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
            appendNetworkLog("Đồng bộ hoàn tất: Thư mục chia sẻ hiện có "
                    + (localFiles == null ? 0 : localFiles.size()) + " tệp.");
        });

        copyTask.setOnFailed(e -> {
            Throwable ex = copyTask.getException();
            String err = (ex != null) ? ex.getMessage() : "Lỗi I/O";
            appendNetworkLog("Lỗi khi xử lý tệp/thư mục kéo thả: " + err);
            setStatus("Lỗi sao chép/nén tệp kéo thả: " + err);
        });

        bindUiToTask(copyTask);
        Thread t = new Thread(copyTask, "p2p-drag-drop-copy-worker");
        t.setDaemon(true);
        t.start();
    }

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

    // ------------------------------------------------------------------------------------------
    // BIỂU ĐỒ TỐC ĐỘ THỜI GIAN THỰC (BOTTOM DOCK AREACHART) & BẢNG LỊCH SỬ CSV
    // ------------------------------------------------------------------------------------------

    private void initializeSpeedTelemetryChart() {
        if (speedTelemetryChart == null || speedTimeAxis == null) {
            return;
        }
        speedSeries = new XYChart.Series<>();
        speedSeries.setName("Tốc độ truyền tải (MB/s)");
        speedTelemetryChart.getData().clear();
        speedTelemetryChart.getData().add(speedSeries);

        speedSeries.getData().add(new XYChart.Data<>(0, 0.0));

        speedChartTimeline = new Timeline(new KeyFrame(Duration.seconds(1.0), event -> {
            chartElapsedSeconds++;
            double currentSpeedMBps = latestSpeedSampleMBps.getAndSet(0.0);
            double roundedSpeed = Math.round(currentSpeedMBps * 100.0) / 100.0;

            if (liveSpeedBadgeLabel != null) {
                liveSpeedBadgeLabel.setText(String.format(Locale.US, "%.2f MB/s", roundedSpeed));
            }

            speedSeries.getData().add(new XYChart.Data<>(chartElapsedSeconds, roundedSpeed));

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

    private void recordSpeedSample(double speedMBps) {
        if (speedMBps < 0) {
            return;
        }
        latestSpeedSampleMBps.accumulateAndGet(speedMBps, Math::max);
    }

    private void initializeHistoryTable() {
        if (historyTable == null) {
            return;
        }
        historyTimeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().timestamp()));
        historyFileColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().fileName()));

        historySizeColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSize()));
        historySizeColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("cell-right-mono");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

        historyPeerColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().peerEndpoint()));
        historyPeerColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("mono-text");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

        historyDirectionColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().direction()));

        historySpeedColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedSpeed()));
        historySpeedColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("cell-right-mono");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

        historyDurationColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().formattedDuration()));
        historyDurationColumn.setCellFactory(col -> new TableCell<>() {
            {
                getStyleClass().add("cell-right-mono");
            }

            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty ? null : item);
            }
        });

        historyHashStatusColumn.setCellValueFactory(cell -> new ReadOnlyStringWrapper(cell.getValue().sha256Status()));
        historyHashStatusColumn.setCellFactory(col -> new TableCell<>() {
            private final Label statusPill = new Label();

            @Override
            protected void updateItem(String status, boolean empty) {
                super.updateItem(status, empty);
                if (empty || status == null) {
                    setGraphic(null);
                    setText(null);
                } else {
                    boolean valid = status.startsWith("Hợp lệ");
                    statusPill.setText(status);
                    statusPill.getStyleClass().setAll("network-pill", valid ? "pill-online" : "pill-error");
                    setGraphic(statusPill);
                    setText(null);
                }
            }
        });

        historyTable.setPlaceholder(createCraftedPlaceholder(
                Feather.BAR_CHART_2,
                "Chưa có phiên truyền tải nào hoàn tất",
                "Các thông số tốc độ MB/s, thời gian hoàn thành (ms) và kiểm định SHA-256 sẽ tự động lưu tại đây"
        ));
        historyTable.setItems(historyList);

        if (historyCountLabel != null) {
            historyCountLabel.textProperty().bind(
                    Bindings.size(historyList).asString("(%d phiên hoàn tất)"));
        }
    }

    private void recordTransferHistory(TransferHistoryItem item) {
        if (item == null) {
            return;
        }
        runOnFxThread(() -> historyList.add(0, item));
    }

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
    // KHỞI TẠO MẠNG & CÁC SỰ KIỆN GIAO DIỆN CỐT LÕI
    // ------------------------------------------------------------------------------------------

    private void refreshHeaderIpBadges() {
        String lanIp = DiscoveryService.detectLocalLanIp();
        String radminIp = DiscoveryService.detectRadminVpnIp();

        if (lanIpPillLabel != null) {
            lanIpPillLabel.setText("LAN: " + lanIp);
        }
        if (radminIpPillLabel != null) {
            boolean radminActive = radminIp != null && radminIp.startsWith("26.");
            radminIpPillLabel.setText("Radmin: " + (radminActive ? radminIp : "OFF"));
            radminIpPillLabel.getStyleClass().setAll("network-pill", radminActive ? "pill-online" : "pill-neutral");
            radminIpPillLabel.setGraphic(icon(Feather.SHIELD, 12, radminActive ? "#10B981" : "#8B949E"));
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
                        firewallConfigButton.setText("Firewall: Ready");
                        firewallConfigButton.setGraphic(icon(Feather.CHECK_CIRCLE, 13, "#10B981"));
                    }
                    appendNetworkLog("Windows Firewall: Đã có sẵn Rule (profile=any) cho phép UDP 8888 & TCP 8889.");
                } else {
                    appendNetworkLog("Gợi ý: Bấm 'Firewall (1-Click)' ở thanh trên cùng để mở cổng cho Radmin VPN & LAN.");
                }
            });
        }, "p2p-firewall-checker");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Tìm kiếm ngữ nghĩa đa từ khóa (Multi-Token &amp; Accent-Insensitive Semantic Search — Phase 2):
     * So khớp đồng thời trên {@code fileName}, {@code aiTags}, {@code aiSummary}, {@code ownerName},
     * {@code ownerIp} và {@code sha256Hash} (hỗ trợ cả tiếng Việt có dấu và không dấu).
     */
    private static boolean matchesSearchKeyword(SharedFile file, String keyword) {
        if (keyword == null || keyword.isBlank()) {
            return true;
        }
        String rawCorpus = (file.fileName() + " "
                + file.aiTags() + " "
                + file.aiSummary() + " "
                + file.ownerName() + " "
                + file.ownerIp() + " "
                + file.sha256Hash()).toLowerCase(Locale.ROOT);

        String lowerKeyword = keyword.toLowerCase(Locale.ROOT).trim();
        if (rawCorpus.contains(lowerKeyword)) {
            return true;
        }

        String normalizedCorpus = HeuristicOfflineAIService.stripVietnameseAccents(rawCorpus);
        String normalizedQuery = HeuristicOfflineAIService.stripVietnameseAccents(lowerKeyword);
        if (normalizedCorpus.contains(normalizedQuery)) {
            return true;
        }

        // Tách câu truy vấn tự nhiên thành các token ngữ nghĩa và kiểm tra khớp tất cả các từ khóa chính
        String[] tokens = normalizedQuery.split("\\s+");
        if (tokens.length > 1) {
            for (String token : tokens) {
                if (token.length() >= 2 && !normalizedCorpus.contains(token)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

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
        p2pServer.setChatMessageCallback(this::appendChatLog);
        p2pServer.setPushApprovalHandler(this::promptUserForPushApproval);

        boolean pinEnabled = pinEnableCheckBox != null && pinEnableCheckBox.isSelected();
        String currentPin = (myPinTextField != null) ? myPinTextField.getText() : "";
        p2pServer.updateSecurityPinConfig(pinEnabled, currentPin);

        boolean askPush = askPushConfirmCheckBox != null && askPushConfirmCheckBox.isSelected();
        p2pServer.setAskBeforeAcceptingPush(askPush);

        p2pServer.setStatusMessageCallback(msg -> {
            if (!downloadManager.isDownloading()) {
                runOnFxThread(() -> setStatus(msg));
            }
        });

        AppThreadPool.execute(p2pServer);
        appendNetworkLog("TCP Multi-Command Server lắng nghe cổng " + P2PServer.DEFAULT_PORT
                + " (PING, LIST_FILES, DOWNLOAD_FILE, DOWNLOAD_CHUNK, PUSH_FILE, CHAT_MESSAGE + AES-256).");
    }

    public void shutdown() {
        if (speedChartTimeline != null) {
            speedChartTimeline.stop();
        }
        sharedFolderManager.stopWatcher();
        AIManager.getInstance().shutdown();
        if (discoveryService != null) {
            discoveryService.stop();
        }
        if (p2pServer != null) {
            p2pServer.stopServer();
        }
        AppThreadPool.shutdownAll();
    }

    // ------------------------------------------------------------------------------------------
    // PHASE 3: PRE-DOWNLOAD 8KB AI PREVIEW, AI NETWORK DIAGNOSTICS & PROVIDER CONFIG
    // ------------------------------------------------------------------------------------------

    private void refreshAiProviderPill() {
        if (aiProviderPillButton == null) {
            return;
        }
        String badgeText = AIManager.getInstance().getActiveProviderBadgeText();
        aiProviderPillButton.setText(badgeText);
        boolean cloudActive = AIManager.getInstance().getGeminiService().isAvailable();
        aiProviderPillButton.getStyleClass().setAll("network-pill", cloudActive ? "pill-online" : "pill-sync");
        aiProviderPillButton.setGraphic(icon(Feather.CPU, 12, cloudActive ? "#10B981" : "#3B82F6"));
    }

    /**
     * Cho phép người dùng bấm vào Pill {@code AI: ...} trên thanh Header để cấu hình nhanh
     * {@code GEMINI_API_KEY} (hoặc để trống để chạy 100% Offline qua Ollama / Local Heuristic).
     */
    @FXML
    private void handleConfigureAiProvider() {
        String currentKey = AIManager.getInstance().getGeminiService().getApiKey();
        TextInputDialog dialog = new TextInputDialog(currentKey);
        dialog.setTitle("Cấu hình Động cơ Trí tuệ Nhân tạo (AI Strategy Config)");
        dialog.setHeaderText("Đang hoạt động: " + AIManager.getInstance().getActiveProviderBadgeText()
                + " | Cache SHA-256: " + AIManager.getInstance().getCacheStore().size() + " tệp");
        dialog.setContentText("Nhập Gemini API Key (để trống nếu muốn dùng Ollama Local / Offline Heuristic):");

        Optional<String> result = dialog.showAndWait();
        if (result.isPresent()) {
            String newKey = result.get().trim();
            AIManager.getInstance().configureGeminiApiKey(newKey);
            refreshAiProviderPill();
            String activeMode = AIManager.getInstance().getActiveProviderBadgeText();
            appendNetworkLog("Đã cập nhật chiến lược AI -> " + activeMode);
            setStatus("Đã chuyển chế độ AI sang: " + activeMode);
        }
    }

    /**
     * Tính năng 2 (Phase 3): <b>Tóm tắt tệp trước khi tải chỉ với 8 KB (Pre-download AI Preview)</b>.
     * Chỉ tải đúng 8 KB đầu tiên từ Peer qua lệnh {@code DOWNLOAD_CHUNK} ({@code offset=0, length=8192})
     * để trích xuất nội dung và phân tích bằng AI, tiết kiệm 99,99% băng thông trước khi tải file nặng.
     */
    @FXML
    private void handleAiPreviewSelectedFile() {
        SharedFile selectedFile = sharedFileTable.getSelectionModel().getSelectedItem();
        if (selectedFile == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn tệp để xem trước");
            alert.setHeaderText("Vui lòng chọn một tệp trong bảng danh sách ở giữa!");
            alert.setContentText("Hãy bấm chọn một tệp trước khi bấm \"Tóm tắt AI (8KB)\".");
            alert.showAndWait();
            return;
        }

        if (aiPreviewFileButton != null) {
            aiPreviewFileButton.setDisable(true);
        }
        String targetEndpoint = selectedFile.resolvedTargetIps().get(0);
        appendNetworkLog("[AI PREVIEW 8KB] Đang trích xuất 8 KB đầu tiên của \""
                + selectedFile.fileName() + "\" từ " + (selectedFile.isLocal() ? "máy cục bộ" : targetEndpoint) + "...");

        Task<String> previewTask = new Task<>() {
            @Override
            protected String call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang lấy 8 KB đầu tiên của \"" + selectedFile.fileName() + "\" để tóm tắt AI...");

                String snippet;
                if (selectedFile.isLocal()) {
                    File localFile = sharedFolderManager.resolveSharedFileOnDisk(selectedFile.fileName());
                    snippet = FileContentExtractor.extractSnippet(localFile);
                } else {
                    snippet = P2PServer.requestPreviewSnippetFromPeer(
                            targetEndpoint,
                            selectedFile.fileName(),
                            getPeerAuthPinInput(),
                            8 * 1024
                    );
                }

                String aiReport = AIManager.getInstance().generatePreDownloadPreviewAsync(
                        selectedFile.fileName(),
                        selectedFile.fileSize(),
                        selectedFile.aiTags(),
                        snippet
                ).get(10, TimeUnit.SECONDS);

                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Đã hoàn tất tóm tắt AI (8KB Preview) cho \"" + selectedFile.fileName() + "\"");
                return aiReport;
            }
        };

        previewTask.setOnSucceeded(e -> {
            if (aiPreviewFileButton != null) {
                aiPreviewFileButton.setDisable(false);
            }
            String report = previewTask.getValue();
            appendNetworkLog("[AI PREVIEW 8KB] Hoàn tất phân tích trước khi tải cho \"" + selectedFile.fileName() + "\".");

            Alert dialog = new Alert(Alert.AlertType.CONFIRMATION);
            dialog.setTitle("Tóm tắt Tệp Trước Khi Tải — Pre-download 8KB AI Preview");
            dialog.setHeaderText(selectedFile.fileName() + " (" + selectedFile.formattedSize()
                    + ") — Nguồn: " + selectedFile.ownerDisplay());

            TextArea reportArea = new TextArea(report);
            reportArea.setEditable(false);
            reportArea.setWrapText(true);
            reportArea.setPrefWidth(620);
            reportArea.setPrefHeight(290);
            reportArea.getStyleClass().add("telemetry-log-area");
            dialog.getDialogPane().setContent(reportArea);

            ButtonType downloadNowBtn = new ButtonType("Tải toàn bộ tệp ngay");
            ButtonType closeBtn = new ButtonType("Đóng");
            dialog.getButtonTypes().setAll(downloadNowBtn, closeBtn);

            Optional<ButtonType> choice = dialog.showAndWait();
            if (choice.isPresent() && choice.get() == downloadNowBtn) {
                handleDownloadSelectedFile();
            }
        });

        previewTask.setOnFailed(e -> {
            if (aiPreviewFileButton != null) {
                aiPreviewFileButton.setDisable(false);
            }
            Throwable ex = previewTask.getException();
            String err = (ex != null && ex.getMessage() != null) ? ex.getMessage() : "Không lấy được chunk 8KB";
            appendNetworkLog("[AI PREVIEW LỖI] " + err);
            setStatus("Lỗi xem trước 8KB: " + err);
        });

        bindUiToTask(previewTask);
        Thread t = new Thread(previewTask, "p2p-ai-8kb-preview-worker");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Tính năng 3 (Phase 3): <b>Chẩn đoán Lỗi Mạng &amp; Tối ưu Băng thông Thông minh (AI Network Diagnostics)</b>.
     * Đọc trực tiếp log lỗi thực tế, trạng thái Firewall, danh sách Subnet, độ trễ Ping RTT và cấu hình QoS
     * để giải thích nguyên nhân gốc rễ và đề xuất cách khắc phục bằng tiếng Việt.
     */
    @FXML
    private void handleAiDiagnoseNetwork() {
        if (aiDiagnoseNetworkButton != null) {
            aiDiagnoseNetworkButton.setDisable(true);
        }

        String rawLogs = (networkLogArea != null && networkLogArea.getText() != null)
                ? networkLogArea.getText()
                : "";
        String[] allLines = rawLogs.split("\r?\n");
        int startLine = Math.max(0, allLines.length - 25);
        StringBuilder recentLogs = new StringBuilder();
        for (int i = startLine; i < allLines.length; i++) {
            recentLogs.append(allLines[i]).append("\n");
        }

        StringBuilder peerLatencySummary = new StringBuilder();
        for (Peer p : peerList) {
            if (!peerLatencySummary.isEmpty()) {
                peerLatencySummary.append(", ");
            }
            peerLatencySummary.append(p.hostName()).append("@").append(p.endpoint())
                    .append("[RTT=").append(p.latencyMs() >= 0 ? (p.latencyMs() + "ms") : "N/A").append("]");
        }

        StringBuilder historySummary = new StringBuilder();
        int histLimit = Math.min(5, historyList.size());
        for (int i = 0; i < histLimit; i++) {
            TransferHistoryItem item = historyList.get(i);
            historySummary.append(item.fileName()).append(" (").append(item.formattedSpeed())
                    .append(", ").append(item.sha256Status()).append("); ");
        }

        int peersCountSnapshot = peerList.size();
        long qosBytesPerSec = BandwidthLimiter.getInstance().getMaxBytesPerSecond();

        Task<String> diagTask = new Task<>() {
            @Override
            protected String call() throws Exception {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang thu thập thông số Firewall, RTT, QoS và phân tích log mạng bằng AI...");

                boolean fwOk = FirewallUtil.isFirewallRuleConfigured();
                List<DiscoveryService.LanSubnetTarget> subnets = DiscoveryService.collectPhysicalLanSubnets();
                StringBuilder subnetStr = new StringBuilder();
                for (DiscoveryService.LanSubnetTarget s : subnets) {
                    subnetStr.append(s.interfaceName()).append("(").append(s.localAddress().getHostAddress()).append(") ");
                }

                NetworkDiagnosticContext ctx = new NetworkDiagnosticContext(
                        recentLogs.toString(),
                        subnetStr.toString().trim(),
                        fwOk,
                        peersCountSnapshot,
                        peerLatencySummary.toString(),
                        qosBytesPerSec,
                        historySummary.toString()
                );

                String diagnosis = AIManager.getInstance().diagnoseNetworkIssueAsync(ctx).get(10, TimeUnit.SECONDS);
                updateProgress(1.0, 1.0);
                updateMessage("Trạng thái: Hoàn tất chẩn đoán mạng thông minh (AI Network Diagnostics).");
                return diagnosis;
            }
        };

        diagTask.setOnSucceeded(e -> {
            if (aiDiagnoseNetworkButton != null) {
                aiDiagnoseNetworkButton.setDisable(false);
            }
            String report = diagTask.getValue();
            appendNetworkLog("\n" + report);

            Alert dialog = new Alert(Alert.AlertType.INFORMATION);
            dialog.setTitle("Báo cáo Chẩn đoán Mạng & Tối ưu Băng thông (AI Network Diagnostics)");
            dialog.setHeaderText("Phân tích tự động từ Telemetry Log, Windows Firewall & Ping RTT ("
                    + AIManager.getInstance().getActiveProviderBadgeText() + ")");

            TextArea area = new TextArea(report);
            area.setEditable(false);
            area.setWrapText(true);
            area.setPrefWidth(620);
            area.setPrefHeight(280);
            area.getStyleClass().add("telemetry-log-area");
            dialog.getDialogPane().setContent(area);
            dialog.showAndWait();
        });

        diagTask.setOnFailed(e -> {
            if (aiDiagnoseNetworkButton != null) {
                aiDiagnoseNetworkButton.setDisable(false);
            }
            setStatus("Lỗi khi chạy chẩn đoán mạng AI.");
        });

        bindUiToTask(diagTask);
        Thread t = new Thread(diagTask, "p2p-ai-network-diagnostics");
        t.setDaemon(true);
        t.start();
    }

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

    @FXML
    private void handleConfigureFirewall() {
        if (firewallConfigButton != null) {
            firewallConfigButton.setDisable(true);
        }
        appendNetworkLog("Đang yêu cầu quyền Administrator (UAC) để mở cổng UDP 8888 & TCP 8889 (profile=any)...");
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
                    firewallConfigButton.setText("Firewall: Ready");
                    firewallConfigButton.setGraphic(icon(Feather.CHECK_CIRCLE, 13, "#10B981"));
                }
                String msg = "Đã thêm Rule Inbound & Outbound (profile=any) vào Windows Firewall!";
                setStatus(msg);
                appendNetworkLog(msg);

                Alert alert = new Alert(Alert.AlertType.INFORMATION);
                alert.setTitle("Cấu hình Windows Firewall thành công");
                alert.setHeaderText("Đã mở cổng UDP 8888 & TCP 8889 cho cả mạng LAN và Radmin VPN!");
                alert.setContentText("Rule đã được áp dụng với profile=any (Private, Public, Domain).");
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
            deepScanButton.setText("Đang quét...");
        }

        Task<List<Peer>> scanTask = new Task<>() {
            @Override
            protected List<Peer> call() {
                updateProgress(-1, 1);
                updateMessage("Trạng thái: Đang quét sâu toàn bộ lớp mạng LAN / Radmin (50 luồng TCP, đo RTT ms)...");
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
                deepScanButton.setText("Quét LAN");
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
                deepScanButton.setText("Quét LAN");
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
        updateThemeButtonGraphic(isDark);
        appendNetworkLog("Đã chuyển đổi chế độ giao diện sang: " + (isDark ? "Primer Dark (Surface #0D1117)" : "Primer Light (Surface #F8FAFC)"));
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
                        combined.addAll(P2PServer.requestFileListFromPeer(peer, getPeerAuthPinInput()));
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
        AppThreadPool.execute(refreshTask);
    }

    @FXML
    private void handleDownloadSelectedFile() {
        SharedFile selectedFile = sharedFileTable.getSelectionModel().getSelectedItem();
        if (selectedFile == null) {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Chưa chọn tệp để tải");
            alert.setHeaderText("Vui lòng chọn một tệp trong bảng danh sách ở giữa!");
            alert.setContentText("Hãy bấm chọn một tệp từ danh sách trước khi bấm \"Tải tệp\".");
            alert.showAndWait();
            return;
        }

        Task<DownloadResult> downloadTask = downloadManager.startDownload(selectedFile);
        if (downloadTask != null) {
            attachDownloadAuthFailureAlert(downloadTask, selectedFile.ownerDisplay());
            bindUiToTask(downloadTask);
        }
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
        Task<DownloadResult> resumedTask = downloadManager.resumeCurrentDownload();
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
            alert.setHeaderText("Vui lòng chọn một thiết bị Peer ở Radar bên trái!");
            alert.setContentText("Bạn cũng có thể kéo thả tệp hoặc cả thư mục trực tiếp vào thẻ Peer ở cột bên trái.");
            alert.showAndWait();
            return;
        }

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Chọn tệp để gửi tới " + selectedPeer.hostName());
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

    private void attachDownloadAuthFailureAlert(Task<DownloadResult> task, String endpointLabel) {
        if (task == null) {
            return;
        }
        task.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal == DownloadResult.AUTH_FAILED) {
                runOnFxThread(() -> showPinAuthFailedAlert(endpointLabel, "Mã PIN xác thực không chính xác hoặc đã bị từ chối/khóa IP."));
            }
        });
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
                + "\n\nHướng dẫn: Hãy hỏi mã PIN 4 chữ số (hoặc Mã Mời #PIN) của đối tác và nhập vào ô 'PIN đối tác' ở cột bên trái.");
        alert.show();
    }

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
        AppThreadPool.execute(scanTask);
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
                        fetched.addAll(P2PServer.requestFileListFromPeer(peer, getPeerAuthPinInput()));
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

        AppThreadPool.execute(listTask);
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
            sharedFolderLabel.setText("Shared: " + sharedFolderManager.getSharedFolderPath().getFileName()
                    + " (NIO WatchService Active)");
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
