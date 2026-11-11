package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;

import java.io.IOException;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dịch vụ tự động phát hiện Peer trong mạng LAN bằng UDP Broadcast.
 *
 * <h2>Cơ chế hoạt động</h2>
 * <ul>
 *   <li><b>BroadcastTask</b>: cứ mỗi {@value #BROADCAST_INTERVAL_MS} ms gửi gói HELLO chứa
 *       tên máy tới 255.255.255.255 và địa chỉ broadcast của từng card mạng.</li>
 *   <li><b>ListenTask</b>: một luồng riêng chặn (blocking) trên {@link DatagramSocket#receive}
 *       để nhận gói tin từ các Peer khác.</li>
 *   <li><b>PruneTask</b>: định kỳ loại bỏ Peer đã quá {@value #PEER_TIMEOUT_MS} ms không phát tín hiệu
 *       (trường hợp máy kia rớt mạng / tắt đột ngột không kịp gửi BYE).</li>
 * </ul>
 *
 * <h2>Định dạng gói tin (UTF-8, phân tách bằng '|')</h2>
 * <pre>NHANNV-P2P|1|HELLO|&lt;instanceId&gt;|&lt;hostName&gt;
 * NHANNV-P2P|1|BYE|&lt;instanceId&gt;|&lt;hostName&gt;</pre>
 * Tiền tố + phiên bản giúp bỏ qua các gói UDP "lạ" của ứng dụng khác dùng chung cổng.
 * {@code instanceId} (UUID ngẫu nhiên mỗi lần chạy) giúp nhận ra và bỏ qua gói tin do chính mình gửi.
 */
public final class DiscoveryService implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DiscoveryService.class.getName());

    public static final int DEFAULT_PORT = 8888;

    private static final String PROTOCOL_PREFIX = "NHANNV-P2P";
    private static final String PROTOCOL_VERSION = "1";
    private static final String TYPE_HELLO = "HELLO";
    private static final String TYPE_BYE = "BYE";
    private static final String SEPARATOR = "|";

    private static final long BROADCAST_INTERVAL_MS = 3_000;
    /** Hết hạn sau ~3 lần broadcast bị lỡ, tránh xóa nhầm khi mất 1-2 gói UDP. */
    private static final long PEER_TIMEOUT_MS = 10_000;
    private static final long PRUNE_INTERVAL_MS = 2_000;
    /** Gói discovery rất nhỏ; 512 byte là quá đủ và an toàn dưới MTU. */
    private static final int MAX_PACKET_SIZE = 512;
    private static final long THREAD_JOIN_TIMEOUT_MS = 1_000;

    private final int port;
    private final PeerDiscoveryListener listener;
    private final String instanceId = UUID.randomUUID().toString();

    /** Danh sách Peer đã biết, key = IP. ConcurrentHashMap vì được truy cập từ nhiều luồng. */
    private final Map<String, Peer> knownPeers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Tên máy được phân giải lười (lazy) trên luồng nền vì getLocalHost() có thể chậm do DNS. */
    private volatile String localHostName;

    private DatagramSocket listenSocket;
    private DatagramSocket sendSocket;
    private Thread listenThread;
    private ScheduledExecutorService scheduler;

    public DiscoveryService(PeerDiscoveryListener listener) {
        this(DEFAULT_PORT, listener);
    }

    public DiscoveryService(int port, PeerDiscoveryListener listener) {
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Port không hợp lệ: " + port);
        }
        this.port = port;
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Mở socket và khởi động các luồng nền. Phương thức trả về ngay (không chặn UI).
     *
     * @throws BindException nếu cổng UDP đã bị ứng dụng khác (hoặc một instance khác) chiếm
     * @throws IOException   nếu không thể mở socket vì lý do khác
     */
    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        // 1) Socket LẮNG NGHE: bind cố định vào cổng discovery trên mọi interface (0.0.0.0).
        //    Nếu cổng đang bị chiếm -> BindException được ném thẳng cho tầng trên xử lý.
        DatagramSocket ls = new DatagramSocket(null);
        try {
            ls.bind(new InetSocketAddress(port));
        } catch (SocketException e) {
            ls.close();
            throw e;
        }

        // 2) Socket GỬI: cổng ngẫu nhiên, bật SO_BROADCAST để được phép gửi tới địa chỉ broadcast.
        //    Tách riêng socket gửi/nhận để luồng nghe không bao giờ bị ảnh hưởng bởi lỗi gửi.
        DatagramSocket ss;
        try {
            ss = new DatagramSocket();
            ss.setBroadcast(true);
        } catch (SocketException e) {
            ls.close();
            throw e;
        }

        listenSocket = ls;
        sendSocket = ss;
        running.set(true);

        // 3) Luồng nghe: daemon để JVM vẫn thoát được kể cả khi quên gọi stop().
        listenThread = new Thread(new ListenTask(), "p2p-discovery-listen");
        listenThread.setDaemon(true);
        listenThread.start();

        // 4) Một luồng scheduler duy nhất chạy tuần tự BroadcastTask và PruneTask,
        //    nên hai tác vụ này không bao giờ chạy chồng lên nhau.
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "p2p-discovery-broadcast");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(new BroadcastTask(), 0, BROADCAST_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(new PruneTask(), PRUNE_INTERVAL_MS, PRUNE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        LOG.info(() -> "DiscoveryService đã khởi động trên UDP port " + port);
    }

    /**
     * Dừng service an toàn: báo BYE cho các Peer, dừng luồng, đóng socket để giải phóng cổng.
     * Có thể gọi nhiều lần (idempotent).
     */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // Gửi BYE (best-effort) để các máy khác xóa mình khỏi danh sách ngay, không phải chờ timeout.
        if (localHostName != null) {
            sendToAllBroadcastAddresses(buildMessage(TYPE_BYE, localHostName));
        }

        // Dừng scheduler trước để không còn tác vụ nào dùng sendSocket sau khi đóng.
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(THREAD_JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Đóng listenSocket sẽ làm receive() đang chặn ném SocketException -> ListenTask tự thoát vòng lặp.
        closeQuietly(listenSocket);
        closeQuietly(sendSocket);

        if (listenThread != null) {
            try {
                listenThread.join(THREAD_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        knownPeers.clear();
        listenSocket = null;
        sendSocket = null;
        listenThread = null;
        scheduler = null;
        LOG.info("DiscoveryService đã dừng và giải phóng port " + port);
    }

    @Override
    public void close() {
        stop();
    }

    // ------------------------------------------------------------------------------------------
    // Các tác vụ nền
    // ------------------------------------------------------------------------------------------

    /** Định kỳ quảng bá sự hiện diện của máy mình ra toàn mạng LAN. */
    private final class BroadcastTask implements Runnable {
        @Override
        public void run() {
            // Bắt mọi ngoại lệ: nếu một lần chạy ném lỗi, ScheduledExecutor sẽ hủy luôn các lần sau.
            try {
                if (!running.get()) {
                    return;
                }
                if (localHostName == null) {
                    localHostName = resolveLocalHostName();
                }
                int sent = sendToAllBroadcastAddresses(buildMessage(TYPE_HELLO, localHostName));
                if (sent == 0 && running.get()) {
                    // Không gửi được tới bất kỳ địa chỉ nào: thường do rớt mạng hoặc Firewall chặn.
                    notifyError("Không gửi được gói Discovery. Kiểm tra kết nối mạng hoặc Firewall.", null);
                }
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Lỗi không mong muốn trong BroadcastTask", e);
            }
        }
    }

    /** Lắng nghe liên tục gói tin Discovery từ các Peer khác. */
    private final class ListenTask implements Runnable {
        @Override
        public void run() {
            // Tái sử dụng một buffer duy nhất cho mọi gói tin để tránh cấp phát liên tục (giảm áp lực GC).
            byte[] buffer = new byte[MAX_PACKET_SIZE];
            DatagramSocket socket = listenSocket;

            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet); // Chặn cho tới khi có gói tin hoặc socket bị đóng.
                    handlePacket(packet);
                } catch (SocketException e) {
                    // Đây là đường thoát BÌNH THƯỜNG khi stop() đóng socket.
                    // Chỉ báo lỗi nếu socket bị đóng bất thường trong lúc service vẫn đang chạy.
                    if (running.get()) {
                        notifyError("Mất kết nối socket lắng nghe Discovery.", e);
                    }
                    break;
                } catch (IOException e) {
                    // Lỗi I/O tạm thời (vd: card mạng chập chờn) -> ghi log và tiếp tục nghe.
                    LOG.log(Level.FINE, "Lỗi I/O khi nhận gói Discovery", e);
                } catch (RuntimeException e) {
                    // Gói tin hỏng không được phép làm chết luồng nghe.
                    LOG.log(Level.FINE, "Bỏ qua gói Discovery không hợp lệ", e);
                }
            }
        }
    }

    /** Loại bỏ các Peer "im lặng" quá lâu (rớt mạng, tắt máy đột ngột). */
    private final class PruneTask implements Runnable {
        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                Iterator<Map.Entry<String, Peer>> it = knownPeers.entrySet().iterator();
                while (it.hasNext()) {
                    Peer peer = it.next().getValue();
                    if (now - peer.lastSeenTimestamp() > PEER_TIMEOUT_MS) {
                        it.remove();
                        notifyPeerLost(peer);
                    }
                }
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Lỗi không mong muốn trong PruneTask", e);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Xử lý gói tin
    // ------------------------------------------------------------------------------------------

    private void handlePacket(DatagramPacket packet) {
        String text = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
        // limit = 5: tên máy (phần cuối) được giữ nguyên dù có chứa ký tự lạ.
        String[] parts = text.split("\\|", 5);
        if (parts.length != 5 || !PROTOCOL_PREFIX.equals(parts[0]) || !PROTOCOL_VERSION.equals(parts[1])) {
            return; // Không phải gói của ứng dụng mình -> bỏ qua.
        }

        String type = parts[2];
        String senderId = parts[3];
        String hostName = parts[4].isBlank() ? "Unknown" : parts[4].trim();

        // Broadcast cũng gửi ngược lại chính máy mình -> nhận diện qua instanceId và bỏ qua.
        if (instanceId.equals(senderId)) {
            return;
        }

        String ip = packet.getAddress().getHostAddress();
        long now = System.currentTimeMillis();

        if (TYPE_HELLO.equals(type)) {
            Peer fresh = new Peer(ip, hostName, now);
            Peer previous = knownPeers.put(ip, fresh);
            // Chỉ báo lên UI khi Peer mới xuất hiện hoặc đổi tên, tránh "spam" cập nhật mỗi 3 giây.
            if (previous == null || !previous.hostName().equals(hostName)) {
                notifyPeerDiscovered(fresh);
            }
        } else if (TYPE_BYE.equals(type)) {
            Peer removed = knownPeers.remove(ip);
            if (removed != null) {
                notifyPeerLost(removed);
            }
        }
    }

    private String buildMessage(String type, String hostName) {
        return String.join(SEPARATOR, PROTOCOL_PREFIX, PROTOCOL_VERSION, type, instanceId, hostName);
    }

    /**
     * Gửi một thông điệp tới 255.255.255.255 và địa chỉ broadcast riêng của từng card mạng.
     * <p>
     * Trên Windows, 255.255.255.255 thường chỉ đi ra card mạng mặc định; gửi thêm vào broadcast
     * của từng subnet (vd: 192.168.1.255) giúp các máy trên Wi-Fi/Ethernet/VM đều nhận được.
     *
     * @return số địa chỉ gửi thành công
     */
    private int sendToAllBroadcastAddresses(String message) {
        DatagramSocket socket = sendSocket;
        if (socket == null || socket.isClosed()) {
            return 0;
        }
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        int success = 0;
        for (InetAddress target : collectBroadcastAddresses()) {
            try {
                socket.send(new DatagramPacket(data, data.length, target, port));
                success++;
            } catch (IOException e) {
                // Một card mạng lỗi không được làm hỏng việc gửi qua các card còn lại.
                LOG.log(Level.FINE, "Không gửi được broadcast tới " + target, e);
            }
        }
        return success;
    }

    private static Set<InetAddress> collectBroadcastAddresses() {
        Set<InetAddress> targets = new LinkedHashSet<>();
        try {
            targets.add(InetAddress.getByName("255.255.255.255"));
        } catch (UnknownHostException ignored) {
            // Không thể xảy ra với literal IP.
        }
        try {
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nif.isUp() || nif.isLoopback()) {
                    continue;
                }
                for (InterfaceAddress address : nif.getInterfaceAddresses()) {
                    InetAddress broadcast = address.getBroadcast(); // null với IPv6
                    if (broadcast != null) {
                        targets.add(broadcast);
                    }
                }
            }
        } catch (SocketException e) {
            LOG.log(Level.FINE, "Không liệt kê được card mạng", e);
        }
        return targets;
    }

    private static String resolveLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // Fallback khi DNS/hosts cấu hình lỗi.
            String env = System.getenv("COMPUTERNAME");
            return env != null && !env.isBlank() ? env : "Unknown-Peer";
        }
    }

    // ------------------------------------------------------------------------------------------
    // Gọi listener an toàn: lỗi ở tầng trên không được làm chết luồng mạng.
    // ------------------------------------------------------------------------------------------

    private void notifyPeerDiscovered(Peer peer) {
        try {
            listener.onPeerDiscovered(peer);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Listener ném lỗi trong onPeerDiscovered", e);
        }
    }

    private void notifyPeerLost(Peer peer) {
        try {
            listener.onPeerLost(peer);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Listener ném lỗi trong onPeerLost", e);
        }
    }

    private void notifyError(String message, Throwable cause) {
        try {
            listener.onDiscoveryError(message, cause);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Listener ném lỗi trong onDiscoveryError", e);
        }
    }

    private static void closeQuietly(DatagramSocket socket) {
        if (socket != null) {
            socket.close();
        }
    }
}
