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
 * Dịch vụ phát hiện Peer trong mạng LAN nội bộ và mạng LAN ảo <b>Radmin VPN (26.x.x.x)</b>.
 *
 * <h2>Giải thích Hỗ trợ Radmin VPN &amp; UDP Direct Ping cho vấn đáp đồ án</h2>
 * <ol>
 *   <li><b>Whitelist Card mạng Radmin VPN ({@code 26.x.x.x}):</b>
 *       Mặc dù Radmin VPN tạo một adapter ảo (<i>Famatech Radmin VPN Ethernet Adapter</i>),
 *       hàm {@link #isRadminInterface(NetworkInterface)} luôn <b>CHO PHÉP (Whitelist)</b> mọi card mạng
 *       có tên chứa {@code "Radmin"} hoặc có địa chỉ IPv4 bắt đầu bằng {@code "26."}, trong khi vẫn
 *       chặn các card máy ảo cục bộ (VMware, VirtualBox, WSL, Hyper-V, Docker).</li>
 *   <li><b>Dual Broadcast cho Radmin VPN:</b>
 *       Khi gửi gói UDP Broadcast trên card Radmin ({@code 26.x.x.x}), hệ thống gửi đồng thời tới cả
 *       địa chỉ Broadcast của card ({@code 26.255.255.255}) lẫn {@code 255.255.255.255}.</li>
 *   <li><b>UDP Unicast Direct Ping ({@link #sendDirectPing(String)}):</b>
 *       Gửi trực tiếp gói tin UDP Unicast {@code PING} thẳng tới địa chỉ IP Radmin của bạn cùng nhóm
 *       (không qua Broadcast). Khi máy bên kia nhận được gói {@code PING}, nó lập tức ghi nhận người gửi
 *       và phản hồi lại một gói {@code HELLO} trực tiếp về IP người gọi, giúp cả 2 máy thấy nhau tức thì!</li>
 * </ol>
 */
public final class DiscoveryService implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DiscoveryService.class.getName());

    public static final int DEFAULT_PORT = 8888;

    private static final String PROTOCOL_PREFIX = "NHANNV-P2P";
    private static final String PROTOCOL_VERSION = "1";
    private static final String TYPE_HELLO = "HELLO";
    private static final String TYPE_PING = "PING";
    private static final String TYPE_BYE = "BYE";
    private static final String SEPARATOR = "|";

    private static final long BROADCAST_INTERVAL_MS = 3_000;
    private static final long PEER_TIMEOUT_MS = 15_000;
    private static final long PRUNE_INTERVAL_MS = 3_000;
    private static final int MAX_PACKET_SIZE = 512;
    private static final long THREAD_JOIN_TIMEOUT_MS = 1_000;

    private static final int ACTIVE_SCAN_THREADS = 50;
    private static final int ACTIVE_SCAN_CONNECT_TIMEOUT_MS = 150;

    /** Danh sách từ khóa nhận diện các card máy ảo cục bộ cần loại bỏ (KHÔNG chứa Radmin). */
    private static final List<String> VIRTUAL_NIC_KEYWORDS = List.of(
            "virtual", "vmware", "vbox", "wsl", "hyper-v", "tap", "vpn",
            "vethernet", "docker", "loopback", "npcap", "wan miniport"
    );

    /** Thông tin một nhánh mạng hợp lệ (LAN vật lý hoặc Radmin VPN). */
    public record LanSubnetTarget(
            String interfaceName,
            InetAddress localAddress,
            InetAddress broadcastAddress,
            boolean isRadmin
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
    // TÁC VỤ 1: Gửi trực tiếp gói UDP Unicast PING tới IP chỉ định (Radmin VPN / LAN)
    // ------------------------------------------------------------------------------------------

    /**
     * Gửi trực tiếp một gói tin UDP Unicast {@code "PING"} tới thẳng địa chỉ {@code targetIp}
     * (không qua Broadcast) để xuyên qua mọi giới hạn mạng ảo của Radmin VPN.
     *
     * @param targetIp địa chỉ IPv4 của Peer đích (vd: {@code "26.45.12.88"} hoặc {@code "192.168.1.20"})
     * @return {@code true} nếu gói UDP đã được đẩy ra socket thành công
     */
    public boolean sendDirectPing(String targetIp) {
        if (targetIp == null || targetIp.isBlank()) {
            return false;
        }
        if (localHostName == null) {
            localHostName = resolveLocalHostName();
        }
        String pingMessage = buildMessage(TYPE_PING, localHostName);
        byte[] data = pingMessage.getBytes(StandardCharsets.UTF_8);

        try {
            InetAddress targetAddress = InetAddress.getByName(targetIp.trim());
            DatagramPacket packet = new DatagramPacket(data, data.length, targetAddress, port);

            // 1) Gửi qua sendSocket chung
            DatagramSocket s = sendSocket;
            if (s != null && !s.isClosed()) {
                s.send(packet);
            }

            // 2) Nếu có card Radmin VPN (26.x.x.x), gửi thêm 1 gói ép buộc đi từ IP Radmin cục bộ
            for (LanSubnetTarget subnet : collectPhysicalLanSubnets()) {
                if (subnet.isRadmin() || targetIp.startsWith("26.")) {
                    try (DatagramSocket nicSocket = new DatagramSocket(new InetSocketAddress(subnet.localAddress(), 0))) {
                        nicSocket.send(packet);
                    } catch (IOException ignored) {
                    }
                }
            }
            return true;
        } catch (IOException e) {
            LOG.log(Level.FINE, "Không thể gửi Direct UDP Ping tới " + targetIp, e);
            return false;
        }
    }

    /**
     * Kết nối thử tới {@code candidateIp:tcpPort} để xác thực Peer đang chạy P2PServer
     * và lấy tên máy chính xác (hỗ trợ cả {@code host:port} cho Ngrok/Playit.gg và cờ {@code pinProtected}).
     */
    public Peer connectAndVerifyPeerTcp(String candidateIp, int tcpPort, int timeoutMs) {
        if (candidateIp == null || candidateIp.isBlank()) {
            return null;
        }
        Peer.HostPort hp = Peer.parseHostPort(candidateIp.trim(), tcpPort);
        String cleanHost = hp.host();
        int actualPort = hp.port();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(cleanHost, actualPort), timeoutMs);
            socket.setSoTimeout(3_000);
            socket.setTcpNoDelay(true);

            String remoteHostName = cleanHost;
            boolean pinProtected = false;
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

                dos.writeUTF(P2PServer.CMD_PING);
                dos.writeUTF(localHostName != null ? localHostName : resolveLocalHostName());
                dos.flush();

                String replyHost = dis.readUTF();
                if (replyHost != null && !replyHost.isBlank()) {
                    remoteHostName = replyHost.trim();
                }
                if (dis.available() > 0) {
                    pinProtected = dis.readBoolean();
                }
            } catch (IOException ignored) {
                remoteHostName = "Peer-" + cleanHost;
            }

            Peer verifiedPeer = new Peer(cleanHost, actualPort, remoteHostName, System.currentTimeMillis(), pinProtected);
            String peerKey = verifiedPeer.endpoint();
            Peer previous = knownPeers.put(peerKey, verifiedPeer);
            if (previous == null || !previous.hostName().equals(remoteHostName) || previous.pinProtected() != pinProtected) {
                notifyPeerDiscovered(verifiedPeer);
            }
            return verifiedPeer;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Quét chủ động dải IP cùng lớp mạng (từ {@code .1} đến {@code .254})
     * bằng {@code ExecutorService} 50 luồng với kết nối TCP tới {@code tcpPort} (timeout 150ms).
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
            logScan(logCallback, "Không tìm thấy card mạng LAN hoặc Radmin VPN nào đang kết nối IPv4.");
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

            if (localHostName == null) {
                localHostName = resolveLocalHostName();
            }

            for (int host = 1; host <= 254; host++) {
                final String candidateIp = prefix + host;
                if (localIps.contains(candidateIp)) {
                    continue;
                }

                tasks.add(() -> {
                    sendDirectPing(candidateIp);
                    Peer found = connectAndVerifyPeerTcp(candidateIp, tcpPort, ACTIVE_SCAN_CONNECT_TIMEOUT_MS);
                    if (found != null) {
                        discoveredInScan.add(found);
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

    /** Cho phép {@link P2PServer} báo ngược khi nhận được lệnh PING qua TCP từ một máy khác. */
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
                // Gửi thêm Direct Ping duy trì kết nối tới tất cả các Peer đã biết (đặc biệt hữu ích cho Radmin VPN)
                for (String knownIp : knownPeers.keySet()) {
                    sendDirectPing(knownIp);
                }
                if (sent == 0 && running.get()) {
                    notifyError("Không gửi được gói Discovery. Kiểm tra kết nối Wi-Fi/Radmin VPN hoặc Firewall.", null);
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
                        Peer refreshed = connectAndVerifyPeerTcp(
                                peer.ipAddress(),
                                P2PServer.DEFAULT_PORT,
                                500
                        );
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
    // Xử lý gói tin & Directed Subnet Broadcast (Ưu tiên Radmin VPN + Card vật lý)
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
            return; // Bỏ qua gói tin do chính máy mình phát ra.
        }

        String ip = packet.getAddress().getHostAddress();
        long now = System.currentTimeMillis();

        if (TYPE_HELLO.equals(type) || TYPE_PING.equals(type)) {
            Peer fresh = new Peer(ip, hostName, now);
            Peer previous = knownPeers.put(ip, fresh);
            if (previous == null || !previous.hostName().equals(hostName)) {
                notifyPeerDiscovered(fresh);
            }

            // Nếu nhận được gói Direct PING (hoặc Peer mới), lập tức trả lời lại bằng gói HELLO Unicast
            // để máy bên kia cũng thấy mình ngay lập tức!
            if (TYPE_PING.equals(type) || previous == null) {
                replyUnicastHello(packet.getAddress());
            }
        } else if (TYPE_BYE.equals(type)) {
            Peer removed = knownPeers.remove(ip);
            if (removed != null) {
                notifyPeerLost(removed);
            }
        }
    }

    private void replyUnicastHello(InetAddress targetAddress) {
        if (localHostName == null) {
            localHostName = resolveLocalHostName();
        }
        byte[] data = buildMessage(TYPE_HELLO, localHostName).getBytes(StandardCharsets.UTF_8);
        DatagramSocket s = sendSocket;
        if (s != null && !s.isClosed()) {
            try {
                s.send(new DatagramPacket(data, data.length, targetAddress, port));
            } catch (IOException ignored) {
            }
        }
    }

    private String buildMessage(String type, String hostName) {
        return String.join(SEPARATOR, PROTOCOL_PREFIX, PROTOCOL_VERSION, type, instanceId, hostName);
    }

    /**
     * Gửi gói UDP Broadcast qua tất cả các card mạng vật lý và card <b>Radmin VPN (26.x.x.x)</b>.
     * <p>
     * Đặc biệt với card mạng Radmin VPN ({@code 26.x.x.x}), hàm gửi đồng thời tới cả:
     * <ol>
     *   <li>Địa chỉ Broadcast của card ({@code 26.255.255.255}).</li>
     *   <li>Địa chỉ Broadcast toàn cục ({@code 255.255.255.255}).</li>
     * </ol>
     */
    private int sendDirectedSubnetBroadcasts(String message) {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        int success = 0;

        List<LanSubnetTarget> activeSubnets = collectPhysicalLanSubnets();
        for (LanSubnetTarget target : activeSubnets) {
            try (DatagramSocket nicSocket = new DatagramSocket(new InetSocketAddress(target.localAddress(), 0))) {
                nicSocket.setBroadcast(true);

                // 1) Gửi tới địa chỉ Broadcast của Subnet (vd: 192.168.1.255 hoặc 26.255.255.255)
                DatagramPacket subnetPacket = new DatagramPacket(data, data.length, target.broadcastAddress(), port);
                nicSocket.send(subnetPacket);
                success++;

                // 2) Nếu là card mạng Radmin VPN (26.x.x.x), gửi đồng thời tới cả 26.255.255.255 và 255.255.255.255
                if (target.isRadmin()) {
                    InetAddress radminBroadcast = InetAddress.getByName("26.255.255.255");
                    if (!radminBroadcast.equals(target.broadcastAddress())) {
                        nicSocket.send(new DatagramPacket(data, data.length, radminBroadcast, port));
                    }
                    InetAddress globalBroadcast = InetAddress.getByName("255.255.255.255");
                    nicSocket.send(new DatagramPacket(data, data.length, globalBroadcast, port));
                }
            } catch (IOException e) {
                LOG.log(Level.FINE, "Không gửi được broadcast qua card " + target.interfaceName(), e);
            }
        }

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
     * Duyệt qua tất cả {@link NetworkInterface} đang hoạt động:
     * <ul>
     *   <li><b>LUÔN CHO PHÉP (Whitelist)</b> card mạng có tên chứa {@code "Radmin"} hoặc có IP bắt đầu bằng {@code "26."}.</li>
     *   <li>Bỏ qua các card máy ảo cục bộ (VMware, VirtualBox, WSL, Hyper-V, Docker...).</li>
     * </ul>
     */
    public static List<LanSubnetTarget> collectPhysicalLanSubnets() {
        List<LanSubnetTarget> result = new ArrayList<>();
        try {
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nif.isUp() || nif.isLoopback()) {
                    continue;
                }

                boolean radminNic = isRadminInterface(nif);
                // Nếu KHÔNG phải Radmin VPN thì mới kiểm tra và lọc bỏ card mạng ảo.
                if (!radminNic && (nif.isVirtual() || isVirtualNetworkInterface(nif))) {
                    continue;
                }

                for (InterfaceAddress ia : nif.getInterfaceAddresses()) {
                    InetAddress addr = ia.getAddress();
                    if (!(addr instanceof Inet4Address) || addr.isLoopbackAddress()) {
                        continue;
                    }

                    String ipStr = addr.getHostAddress();
                    boolean isRadminIp = radminNic || ipStr.startsWith("26.");

                    InetAddress broadcast = ia.getBroadcast();
                    if (isRadminIp && broadcast == null) {
                        try {
                            broadcast = InetAddress.getByName("26.255.255.255");
                        } catch (UnknownHostException ignored) {
                        }
                    }

                    if (broadcast != null) {
                        result.add(new LanSubnetTarget(nif.getDisplayName(), addr, broadcast, isRadminIp));
                    }
                }
            }
        } catch (SocketException e) {
            LOG.log(Level.FINE, "Không liệt kê được danh sách NetworkInterface", e);
        }
        return result;
    }

    /**
     * Kiểm tra xem một {@link NetworkInterface} có phải là card mạng của <b>Radmin VPN</b> hay không
     * (dựa trên tên chứa {@code "Radmin"} hoặc sở hữu địa chỉ IPv4 thuộc dải {@code 26.x.x.x}).
     */
    public static boolean isRadminInterface(NetworkInterface nif) {
        String name = (nif.getName() == null ? "" : nif.getName()).toLowerCase(Locale.ROOT);
        String display = (nif.getDisplayName() == null ? "" : nif.getDisplayName()).toLowerCase(Locale.ROOT);
        if (name.contains("radmin") || display.contains("radmin") || display.contains("famatech")) {
            return true;
        }
        for (InterfaceAddress ia : nif.getInterfaceAddresses()) {
            InetAddress addr = ia.getAddress();
            if (addr instanceof Inet4Address && addr.getHostAddress().startsWith("26.")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Kiểm tra tên hoặc mô tả của card mạng có chứa các từ khóa của card máy ảo cục bộ hay không.
     * Lưu ý: Radmin VPN luôn được ưu tiên giữ lại trước khi gọi hàm này.
     */
    public static boolean isVirtualNetworkInterface(NetworkInterface nif) {
        if (isRadminInterface(nif)) {
            return false;
        }
        String name = (nif.getName() == null ? "" : nif.getName()).toLowerCase(Locale.ROOT);
        String display = (nif.getDisplayName() == null ? "" : nif.getDisplayName()).toLowerCase(Locale.ROOT);
        for (String keyword : VIRTUAL_NIC_KEYWORDS) {
            if (name.contains(keyword) || display.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    /** Lấy địa chỉ IP LAN nội bộ (vd: {@code 192.168.1.x}) của máy để hiển thị trên thanh tiêu đề. */
    public static String detectLocalLanIp() {
        for (LanSubnetTarget target : collectPhysicalLanSubnets()) {
            if (!target.isRadmin()) {
                return target.localAddress().getHostAddress();
            }
        }
        return "Chưa kết nối";
    }

    /** Lấy địa chỉ IP Radmin VPN ({@code 26.x.x.x}) của máy (nếu đang bật Radmin VPN). */
    public static String detectRadminVpnIp() {
        for (LanSubnetTarget target : collectPhysicalLanSubnets()) {
            if (target.isRadmin()) {
                return target.localAddress().getHostAddress();
            }
        }
        return "Chưa bật";
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
