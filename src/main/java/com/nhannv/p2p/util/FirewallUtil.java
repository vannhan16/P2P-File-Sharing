package com.nhannv.p2p.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tiện ích tự động kiểm tra và thêm Rule cổng cụ thể vào <b>Windows Defender Firewall</b>
 * để các máy thật trong mạng LAN nội bộ và <b>Radmin VPN (26.x.x.x)</b> thấy nhau,
 * truyền tệp mà <b>KHÔNG CẦN tắt Firewall</b>.
 * <p>
 * <b>Nguyên tắc Bảo mật (Least Privilege):</b> Chỉ mở đúng cổng UDP (mặc định 8888) và cổng TCP (mặc định 8889),
 * <i>KHÔNG</i> mở toàn bộ tiến trình {@code java.exe} ở mọi cổng.
 */
public final class FirewallUtil {

    private static final Logger LOG = Logger.getLogger(FirewallUtil.class.getName());

    public static final String RULE_PREFIX = "VKU_P2P_FileSharing";
    public static final String RULE_UDP_IN = RULE_PREFIX + "_UDP_IN";
    public static final String RULE_UDP_OUT = RULE_PREFIX + "_UDP_OUT";
    public static final String RULE_TCP_IN = RULE_PREFIX + "_TCP_IN";
    public static final String RULE_TCP_OUT = RULE_PREFIX + "_TCP_OUT";

    public static final int DEFAULT_UDP_PORT = 8888;
    public static final int DEFAULT_TCP_PORT = 8889;

    private FirewallUtil() {
    }

    /** Kiểm tra hệ điều hành hiện tại có phải Windows hay không. */
    public static boolean isWindows() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }

    /**
     * Kiểm tra xem toàn bộ 4 Rule Firewall (TCP In/Out, UDP In/Out) của ứng dụng đã được cấu hình chưa.
     */
    public static boolean isFirewallRuleConfigured() {
        return isFirewallRuleConfigured(DEFAULT_UDP_PORT, DEFAULT_TCP_PORT);
    }

    /**
     * Kiểm tra 4 Rule Firewall tương ứng với cổng UDP và TCP đang hoạt động.
     */
    public static boolean isFirewallRuleConfigured(int udpPort, int tcpPort) {
        if (!isWindows()) {
            return true;
        }
        return checkSingleRule(RULE_TCP_IN)
                && checkSingleRule(RULE_TCP_OUT)
                && checkSingleRule(RULE_UDP_IN)
                && checkSingleRule(RULE_UDP_OUT);
    }

    private static boolean checkSingleRule(String ruleName) {
        try {
            Process process = new ProcessBuilder(
                    "netsh", "advfirewall", "firewall", "show", "rule",
                    "name=" + ruleName
            ).redirectErrorStream(true).start();

            boolean finished = process.waitFor(4, TimeUnit.SECONDS);
            return finished && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    /**
     * Mở hộp thoại UAC (Run as Administrator) để thêm đúng bộ 4 Rule Inbound &amp; Outbound
     * với {@code profile=any} (Domain, Private, Public) cho cổng UDP Discovery và cổng TCP P2P.
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
                .append(addPortRuleCmd(RULE_UDP_IN, "in", "UDP", udpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_UDP_OUT, "out", "UDP", udpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_TCP_IN, "in", "TCP", tcpPort)).append(" & ")
                .append(addPortRuleCmd(RULE_TCP_OUT, "out", "TCP", tcpPort));

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
                    // Đọc hết output của tiến trình con
                }
            }

            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            if (!exited) {
                process.destroyForcibly();
                return false;
            }

            return isFirewallRuleConfigured(udpPort, tcpPort);
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
                "netsh advfirewall firewall delete rule name=\"%s\"",
                ruleName);
    }

    private static String addPortRuleCmd(String ruleName, String dir, String protocol, int port) {
        return String.format(Locale.US,
                "netsh advfirewall firewall add rule name=\"%s\" dir=%s action=allow protocol=%s localport=%d enable=yes profile=any",
                ruleName, dir, protocol, port);
    }
}
