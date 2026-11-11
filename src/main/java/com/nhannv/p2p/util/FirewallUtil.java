package com.nhannv.p2p.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tiện ích tự động kiểm tra và thêm Rule vào <b>Windows Defender Firewall</b>
 * để các máy thật trong mạng LAN nội bộ và <b>Radmin VPN (26.x.x.x)</b> thấy nhau,
 * truyền tệp mà <b>KHÔNG CẦN tắt Firewall</b>.
 *
 * <h2>Đảm bảo tương thích Radmin VPN ({@code profile=any})</h2>
 * Khi cài đặt Radmin VPN, Windows thường xếp card mạng ảo <i>Famatech Radmin VPN Ethernet Adapter</i>
 * vào nhóm <b>Unidentified Network (Public Profile)</b>. Vì vậy mọi lệnh {@code netsh advfirewall}
 * bên dưới đều bắt buộc có tham số {@code profile=any} (áp dụng cho cả <i>Domain, Private và Public</i>)
 * kèm quyền cho tiến trình {@code java.exe} ở cả chiều Inbound và Outbound.
 */
public final class FirewallUtil {

    private static final Logger LOG = Logger.getLogger(FirewallUtil.class.getName());

    public static final String RULE_PREFIX = "VKU_P2P_FileSharing";
    private static final String RULE_UDP_IN = RULE_PREFIX + "_UDP_IN";
    private static final String RULE_UDP_OUT = RULE_PREFIX + "_UDP_OUT";
    private static final String RULE_TCP_IN = RULE_PREFIX + "_TCP_IN";
    private static final String RULE_TCP_OUT = RULE_PREFIX + "_TCP_OUT";
    private static final String RULE_JAVA_APP_IN = RULE_PREFIX + "_JAVA_APP_IN";
    private static final String RULE_JAVA_APP_OUT = RULE_PREFIX + "_JAVA_APP_OUT";

    private FirewallUtil() {
    }

    /** Kiểm tra hệ điều hành hiện tại có phải Windows hay không. */
    public static boolean isWindows() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }

    /**
     * Kiểm tra xem Rule Firewall của ứng dụng P2P đã được thêm vào Windows Firewall trước đó chưa.
     */
    public static boolean isFirewallRuleConfigured() {
        if (!isWindows()) {
            return true;
        }
        try {
            Process process = new ProcessBuilder(
                    "netsh", "advfirewall", "firewall", "show", "rule",
                    "name=" + RULE_TCP_IN
            ).redirectErrorStream(true).start();

            boolean finished = process.waitFor(5, TimeUnit.SECONDS);
            return finished && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    /**
     * Mở hộp thoại UAC (Run as Administrator) để tự động thêm bộ Rule Inbound &amp; Outbound
     * với {@code profile=any} (Domain, Private, Public - bao gồm card Radmin VPN)
     * cho cổng UDP Discovery (8888), cổng TCP Transfer (8889) và tiến trình {@code java.exe}.
     *
     * @param udpPort cổng UDP Discovery (mặc định 8888)
     * @param tcpPort cổng TCP P2P Server (mặc định 8889)
     * @return {@code true} nếu người dùng bấm Yes trên hộp thoại UAC và các rule đã được tạo thành công
     */
    public static boolean configureFirewallRulesWithUac(int udpPort, int tcpPort) {
        if (!isWindows()) {
            LOG.info("Không phải hệ điều hành Windows -> bỏ qua cấu hình netsh.");
            return true;
        }

        StringBuilder cmdChain = new StringBuilder();
        cmdChain.append(deleteRuleCmd(RULE_UDP_IN)).append(" & ")
                .append(deleteRuleCmd(RULE_UDP_OUT)).append(" & ")
                .append(deleteRuleCmd(RULE_TCP_IN)).append(" & ")
                .append(deleteRuleCmd(RULE_TCP_OUT)).append(" & ")
                .append(deleteRuleCmd(RULE_JAVA_APP_IN)).append(" & ")
                .append(deleteRuleCmd(RULE_JAVA_APP_OUT)).append(" & ")
                .append(addPortRuleCmd(RULE_UDP_IN, "in", "UDP", udpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_UDP_OUT, "out", "UDP", udpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_TCP_IN, "in", "TCP", tcpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_TCP_OUT, "out", "TCP", tcpPort));

        Optional<String> currentJavaExe = ProcessHandle.current().info().command();
        if (currentJavaExe.isPresent()) {
            String javaPath = currentJavaExe.get();
            cmdChain.append(" & ")
                    .append(String.format(Locale.US,
                            "netsh advfirewall firewall add rule name=\"%s\" dir=in action=allow program=\"%s\" enable=yes profile=any",
                            RULE_JAVA_APP_IN, javaPath))
                    .append(" & ")
                    .append(String.format(Locale.US,
                            "netsh advfirewall firewall add rule name=\"%s\" dir=out action=allow program=\"%s\" enable=yes profile=any",
                            RULE_JAVA_APP_OUT, javaPath));
        }

        String argumentList = "/c " + cmdChain;
        String escapedForSingleQuote = argumentList.replace("'", "''");
        String psCommand = "Start-Process cmd -ArgumentList '" + escapedForSingleQuote + "' -Verb RunAs -Wait -WindowStyle Hidden";

        List<String> command = List.of(
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy", "Bypass",
                "-Command", psCommand
        );

        try {
            Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                while (reader.readLine() != null) {
                    // Đọc hết output của tiến trình con.
                }
            }

            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            if (!exited) {
                process.destroyForcibly();
                return false;
            }

            return isFirewallRuleConfigured();
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.log(Level.WARNING, "Không thể chạy lệnh cấu hình Windows Firewall", e);
            return false;
        }
    }

    private static String deleteRuleCmd(String ruleName) {
        return String.format(Locale.US,
                "netsh advfirewall firewall delete rule name=\"%s\" >nul 2>&1",
                ruleName);
    }

    private static String addPortRuleCmd(String ruleName, String direction, String protocol, int port) {
        return String.format(Locale.US,
                "netsh advfirewall firewall add rule name=\"%s\" dir=%s action=allow protocol=%s localport=%d profile=any interfacetype=any",
                ruleName, direction, protocol, port);
    }
}
