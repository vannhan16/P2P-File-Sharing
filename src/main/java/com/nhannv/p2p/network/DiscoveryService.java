package com.nhannv.p2p.network;

import com.nhannv.p2p.model.Peer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Dịch vụ phát hiện Peer trong mạng LAN bằng <b>Directed Subnet UDP Broadcast</b>
 * kết hợp <b>TCP Active Subnet Scan (Fallback)</b>.
 *
 * <h2>Giải thích Kỹ thuật Mạng cho vấn đáp đồ án (Huỳnh Nhân Danh)</h2>
 * <ol>
 *   <li><b>Tại sao gửi tới {@code 255.255.255.255} thường thất bại trên máy Windows thật?</b>
 *       Trên máy lập trình viên thường cài sẵn VMware, VirtualBox, WSL2, Hyper-V, Docker hoặc VPN.
 *       Khi gửi gói UDP tới địa chỉ mù {@code 255.255.255.255}, bảng định tuyến (Routing Table) của
 *       Windows rất hay đẩy gói tin đó vào một card mạng ảo (có metric thấp hơn) thay vì card Wi-Fi/Ethernet thật!</li>
 *   <li><b>Cách khắc phục triệt để (Directed Subnet Broadcast + Lọc Card Ảo):</b>
 *       <ul>
 *         <li>Duyệt {@link NetworkInterface#getNetworkInterfaces()}, chỉ giữ các card mạng đang bật
 *             ({@code isUp() && !isLoopback() && !isVirtual()}) và loại bỏ toàn bộ card có tên chứa
 *             {@code "Virtual", "VMware", "VBox", "WSL", "Hyper-V", "TAP", "VPN", "Docker"...}.</li>
 *         <li>Với mỗi {@link InterfaceAddress} IPv4 thật, lấy chính xác địa chỉ Broadcast của lớp mạng đó
 *             qua {@code interfaceAddress.getBroadcast()} (ví dụ {@code 192.168.1.255}) và mở socket
 *             gắn trực tiếp với IP cục bộ của card mạng đó để ép Windows phát sóng đúng ra card Wi-Fi/LAN.</li>
 *       </ul>
 *   </li>
 *   <li><b>Cơ chế TCP Active Subnet Scan (Fallback khi Router chặn UDP Broadcast):</b>
 *       Nhiều Router Wi-Fi trường học/ký túc xá bật tính năng <i>Broadcast Storm Filter</i> chặn gói UDP
 *       Broadcast nhưng vẫn cho phép kết nối TCP Unicast giữa các máy cùng lớp mạng.
 *       Hàm {@link #scanSubnetActiveTcp(int, Consumer)} dùng {@link ExecutorService} <b>50 luồng</b>
 *       quét nhanh dải IP {@code .1 -> .254} với timeout {@code 150ms} trên cổng TCP {@code 8889}
 *       để tìm ra mọi Peer đang online chỉ trong chưa đầy 1 giây!</li>
 * </ol>
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
    private static final long PEER_TIMEOUT_MS = 12_000;
    private static final long PRUNE_INTERVAL_MS = 3_000;
    private static final int MAX_PACKET_SIZE = 512;
    private static final long THREAD_JOIN_TIMEOUT_MS = 1_000;

    /** Số luồng quét song song khi chạy TCP Active Subnet Scan. */
    private static final int ACTIVE_SCAN_THREADS = 50;
    /** Timeout kết nối TCP (150ms) đủ nhanh cho mạng LAN nội bộ (< 5ms RTT). */
    private static final int ACTIVE_SCAN_CONNECT_TIMEOUT_MS = 150;

    /** Danh sách từ khóa nhận diện các card mạng ảo cần loại bỏ. */
    private static final List<String> VIRTUAL_NIC_KEYWORDS = List.of(
            "virtual", "vmware", "vbox", "wsl", "hyper-v", "tap", "vpn",
            "vethernet", "docker", "loopback", "npcap", "wan miniport",
            "zerotier", "tailscale", "hamachi", "radmin"
    );

    /** Thông tin một nhánh mạng LAN vật lý (IP cục bộ + Địa chỉ Broadcast của Subnet). */
    public record LanSubnetTarget(
            String interfaceName,
            InetAddress localAddress,
            InetAddress broadcastAddress
    ) {
    }

    private final int port;
    private final PeerDiscoveryListener listener;
    private final String instanceId = UUID.randomUUID().toString();

    private final Map<String, Peer> knownPeers = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

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

    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }

        DatagramSocket ls = new DatagramSocket(null);
        try {
            ls.setReuseAddress(true);
            ls.bind(new InetSocketAddress(port));
        } catch (SocketException e) {
            ls.close();
            throw e;
        }

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

        listenThread = new Thread(new ListenTask(), "p2p-discovery-listen");
        listenThread.setDaemon(true);
        listenThread.start();

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "p2p-discovery-broadcast");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(new BroadcastTask(), 0, BROADCAST_INTERVAL_MS, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(new PruneTask(), PRUNE_INTERVAL_MS, PRUNE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        LOG.info(() -> "DiscoveryService đã khởi động trên UDP port " + port);
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (localHostName != null) {
            sendDirectedSubnetBroadcasts(buildMessage(TYPE_BYE, localHostName));
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(THREAD_JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

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
    // TCP Active Subnet Scan (Fallback khi Router Wi-Fi chặn UDP Broadcast)
    // ------------------------------------------------------------------------------------------

    /**
     * Quét chủ động toàn bộ dải IP cùng lớp mạng (từ {@code .1} đến {@code .254})
     * bằng {@code ExecutorService} 50 luồng với kết nối TCP tới {@code tcpPort} (timeout 150ms).
     * <p>
     * Giúp tìm thấy các máy đang chạy P2P ngay cả khi Router Wi-Fi chặn gói tin UDP Broadcast.
     *
     * @param tcpPort     cổng TCP của {@link P2PServer} (mặc định 8889)
     * @param logCallback callback ghi nhật ký ra màn hình
     * @return danh sách các {@link Peer} tìm thấy qua quét TCP
     */
    public List<Peer> scanSubnetActiveTcp(int tcpPort, Consumer<String> logCallback) {
        List<LanSubnetTarget> subnets = collectPhysicalLanSubnets();
        Set<String> localIps = new LinkedHashSet<>();
        Set<String> subnetPrefixes = new LinkedHashSet<>();

        for (LanSubnetTarget target : subnets) {
            String ip = target.localAddress().getHostAddress();
            localIps.add(ip);
            int lastDot = ip.lastIndexOf('.');
            if (lastDot > 0) {
                subnetPrefixes.add(ip.substring(0, lastDot + 1));
            }
        }

        if (subnetPrefixes.isEmpty()) {
            logScan(logCallback, "Không tìm thấy card mạng LAN vật lý nào đang kết nối IPv4.");
            return List.of();
        }

        AtomicInteger threadIdx = new AtomicInteger(1);
        ExecutorService scanPool = Executors.newFixedThreadPool(ACTIVE_SCAN_THREADS, r -> {
            Thread t = new Thread(r, "p2p-tcp-subnet-scanner-" + threadIdx.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        List<Peer> discoveredInScan = Collections.synchronizedList(new ArrayList<>());
        List<Callable<Void>> tasks = new ArrayList<>();

        for (String prefix : subnetPrefixes) {
            logScan(logCallback, "Đang quét sâu dải mạng " + prefix + "1 -> " + prefix + "254 trên cổng TCP "
                    + tcpPort + " (50 luồng, timeout 150ms)...");

            // Đồng thời bắn 1 loạt UDP Unicast trực tiếp tới từng IP trong dải .1 -> .254
            // để đánh thức ListenTask của các máy khác ngay cả khi Multicast/Broadcast bị chặn.
            if (localHostName == null) {
                localHostName = resolveLocalHostName();
            }
            byte[] helloBytes = buildMessage(TYPE_HELLO, localHostName).getBytes(StandardCharsets.UTF_8);

            for (int host = 1; host <= 254; host++) {
                final String candidateIp = prefix + host;
                if (localIps.contains(candidateIp)) {
                    continue; // Bỏ qua chính IP của máy mình.
                }

                tasks.add(() -> {
                    // 1) Gửi kèm 1 gói UDP Unicast trực tiếp tới candidateIp:8888
                    DatagramSocket s = sendSocket;
                    if (s != null && !s.isClosed()) {
                        try {
                            InetAddress addr = InetAddress.getByName(candidateIp);
                            s.send(new DatagramPacket(helloBytes, helloBytes.length, addr, port));
                        } catch (IOException ignored) {
                        }
                    }

                    // 2) Thử kết nối TCP tới candidateIp:8889 với timeout 150ms
                    Peer found = probePeerViaTcp(candidateIp, tcpPort);
                    if (found != null) {
                        discoveredInScan.add(found);
                        Peer previous = knownPeers.put(found.ipAddress(), found);
                        if (previous == null || !previous.hostName().equals(found.hostName())) {
                            notifyPeerDiscovered(found);
                        }
                    }
                    return null;
                });
            }
        }

        try {
            List<Future<Void>> futures = scanPool.invokeAll(tasks);
            for (Future<Void> f : futures) {
                try {
                    f.get();
                } catch (Exception ignored) {
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scanPool.shutdownNow();
        }

        return new ArrayList<>(discoveredInScan);
    }

    /**
     * Kết nối thử tới {@code candidateIp:tcpPort} trong 150ms và gửi lệnh {@code PING}
     * để lấy tên máy của Peer đó.
     */
    private Peer probePeerViaTcp(String candidateIp, int tcpPort) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(candidateIp, tcpPort), ACTIVE_SCAN_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(1_000);
            socket.setTcpNoDelay(true);

            String remoteHostName = candidateIp;
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(P2PServer.CMD_PING);
                dos.writeUTF(localHostName != null ? localHostName : resolveLocalHostName());
                dos.flush();

                String replyHost = dis.readUTF();
                if (replyHost != null && !replyHost.isBlank()) {
                    remoteHostName = replyHost.trim();
                }
            } catch (IOException ignored) {
                // Nếu Peer chạy phiên bản cũ chưa có lệnh PING, cổng 8889 mở vẫn chứng tỏ là Peer P2P!
                remoteHostName = "Peer-" + candidateIp;
            }

            return new Peer(candidateIp, remoteHostName, System.currentTimeMillis());
        } catch (IOException e) {
            return null; // Không mở cổng TCP 8889 -> không phải Peer P2P.
        }
    }

    /** Cho phép {@link P2PServer} báo ngược khi nhận được lệnh PING từ một máy khác. */
    public void registerPeerFromPing(String ipAddress, String hostName) {
        if (ipAddress == null || ipAddress.isBlank()) {
            return;
        }
        String safeHost = (hostName == null || hostName.isBlank()) ? ipAddress : hostName.trim();
        Peer fresh = new Peer(ipAddress, safeHost, System.currentTimeMillis());
        Peer previous = knownPeers.put(ipAddress, fresh);
        if (previous == null || !previous.hostName().equals(safeHost)) {
            notifyPeerDiscovered(fresh);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Các tác vụ nền (BroadcastTask, ListenTask, PruneTask)
    // ------------------------------------------------------------------------------------------

    private final class BroadcastTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!running.get()) {
                    return;
                }
                if (localHostName == null) {
                    localHostName = resolveLocalHostName();
                }
                int sent = sendDirectedSubnetBroadcasts(buildMessage(TYPE_HELLO, localHostName));
                if (sent == 0 && running.get()) {
                    notifyError("Không gửi được gói Discovery qua card mạng vật lý. Kiểm tra Wi-Fi/LAN hoặc Firewall.", null);
                }
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Lỗi không mong muốn trong BroadcastTask", e);
            }
        }
    }

    private final class ListenTask implements Runnable {
        @Override
        public void run() {
            byte[] buffer = new byte[MAX_PACKET_SIZE];
            DatagramSocket socket = listenSocket;

            while (running.get()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                    handlePacket(packet);
                } catch (SocketException e) {
                    if (running.get()) {
                        notifyError("Mất kết nối socket lắng nghe Discovery.", e);
                    }
                    break;
                } catch (IOException e) {
                    LOG.log(Level.FINE, "Lỗi I/O khi nhận gói Discovery", e);
                } catch (RuntimeException e) {
                    LOG.log(Level.FINE, "Bỏ qua gói Discovery không hợp lệ", e);
                }
            }
        }
    }

    private final class PruneTask implements Runnable {
        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                Iterator<Map.Entry<String, Peer>> it = knownPeers.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Peer> entry = it.next();
                    Peer peer = entry.getValue();
                    if (now - peer.lastSeenTimestamp() > PEER_TIMEOUT_MS) {
                        // Trước khi xóa, kiểm tra nhanh xem cổng TCP 8889 của Peer còn sống không
                        // (để các Peer tìm thấy qua TCP Active Scan không bị xóa nhầm khi Router chặn UDP).
                        Peer refreshed = probePeerViaTcp(peer.ipAddress(), P2PServer.DEFAULT_PORT);
                        if (refreshed != null) {
                            entry.setValue(refreshed);
                        } else {
                            it.remove();
                            notifyPeerLost(peer);
                        }
                    }
                }
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Lỗi không mong muốn trong PruneTask", e);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Xử lý gói tin & Directed Subnet Broadcast (Loại bỏ Card mạng ảo)
    // ------------------------------------------------------------------------------------------

    private void handlePacket(DatagramPacket packet) {
        String text = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
        String[] parts = text.split("\\|", 5);
        if (parts.length != 5 || !PROTOCOL_PREFIX.equals(parts[0]) || !PROTOCOL_VERSION.equals(parts[1])) {
            return;
        }

        String type = parts[2];
        String senderId = parts[3];
        String hostName = parts[4].isBlank() ? "Unknown" : parts[4].trim();

        if (instanceId.equals(senderId)) {
            return; // Bỏ qua gói tin của chính máy mình.
        }

        String ip = packet.getAddress().getHostAddress();
        long now = System.currentTimeMillis();

        if (TYPE_HELLO.equals(type)) {
            Peer fresh = new Peer(ip, hostName, now);
            Peer previous = knownPeers.put(ip, fresh);
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
     * Gửi gói UDP Ping trực tiếp qua từng card mạng vật lý tới đúng địa chỉ Broadcast
     * của từng Subnet ({@code interfaceAddress.getBroadcast()}, vd {@code 192.168.1.255}).
     * <p>
     * Việc bind một {@link DatagramSocket} tạm vào đúng {@code target.localAddress()} ép hệ điều hành
     * Windows định tuyến gói Broadcast đi ra đúng card Wi-Fi / Ethernet vật lý đó, không bao giờ
     * bị kẹt ở card mạng ảo WSL/VMware/VirtualBox!
     */
    private int sendDirectedSubnetBroadcasts(String message) {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        int success = 0;

        List<LanSubnetTarget> physicalSubnets = collectPhysicalLanSubnets();
        for (LanSubnetTarget target : physicalSubnets) {
            try (DatagramSocket nicSocket = new DatagramSocket(new InetSocketAddress(target.localAddress(), 0))) {
                nicSocket.setBroadcast(true);
                DatagramPacket packet = new DatagramPacket(data, data.length, target.broadcastAddress(), port);
                nicSocket.send(packet);
                success++;
            } catch (IOException e) {
                LOG.log(Level.FINE, "Không gửi được broadcast qua card " + target.interfaceName(), e);
            }
        }

        // Nếu máy có cấu hình mạng đặc biệt không lấy được subnet broadcast, fallback về sendSocket chung.
        if (success == 0) {
            DatagramSocket fallbackSocket = sendSocket;
            if (fallbackSocket != null && !fallbackSocket.isClosed()) {
                try {
                    InetAddress globalBroadcast = InetAddress.getByName("255.255.255.255");
                    fallbackSocket.send(new DatagramPacket(data, data.length, globalBroadcast, port));
                    success++;
                } catch (IOException e) {
                    LOG.log(Level.FINE, "Fallback broadcast 255.255.255.255 thất bại", e);
                }
            }
        }

        return success;
    }

    /**
     * Duyệt qua tất cả {@link NetworkInterface} đang hoạt động ({@code isUp() && !isLoopback()}),
     * bỏ qua các card mạng ảo (Virtual, VMware, VBox, WSL, Hyper-V, TAP, VPN...) và trích xuất
     * địa chỉ Broadcast của từng Subnet IPv4 vật lý.
     */
    public static List<LanSubnetTarget> collectPhysicalLanSubnets() {
        List<LanSubnetTarget> result = new ArrayList<>();
        try {
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nif.isUp() || nif.isLoopback() || nif.isVirtual()) {
                    continue;
                }
                if (isVirtualNetworkInterface(nif)) {
                    continue;
                }

                for (InterfaceAddress ia : nif.getInterfaceAddresses()) {
                    InetAddress addr = ia.getAddress();
                    InetAddress broadcast = ia.getBroadcast();
                    if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && broadcast != null) {
                        result.add(new LanSubnetTarget(nif.getDisplayName(), addr, broadcast));
                    }
                }
            }
        } catch (SocketException e) {
            LOG.log(Level.FINE, "Không liệt kê được danh sách NetworkInterface", e);
        }
        return result;
    }

    /**
     * Kiểm tra tên hoặc mô tả của card mạng có chứa các từ khóa của card mạng ảo hay không:
     * {@code "Virtual", "VMware", "VBox", "WSL", "Hyper-V", "TAP", "VPN"...}.
     */
    public static boolean isVirtualNetworkInterface(NetworkInterface nif) {
        String name = (nif.getName() == null ? "" : nif.getName()).toLowerCase(Locale.ROOT);
        String display = (nif.getDisplayName() == null ? "" : nif.getDisplayName()).toLowerCase(Locale.ROOT);
        for (String keyword : VIRTUAL_NIC_KEYWORDS) {
            if (name.contains(keyword) || display.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    private static String resolveLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            String env = System.getenv("COMPUTERNAME");
            return env != null && !env.isBlank() ? env : "Unknown-Peer";
        }
    }

    private static void logScan(Consumer<String> logCallback, String msg) {
        if (logCallback != null) {
            logCallback.accept(msg);
        }
    }

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
