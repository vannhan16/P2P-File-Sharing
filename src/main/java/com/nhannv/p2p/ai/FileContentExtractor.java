package com.nhannv.p2p.ai;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Bộ trích xuất ngữ cảnh siêu nhẹ ({@code <= 4 KB}) từ tệp trên đĩa hoặc từ luồng 8 KB chunk tải thử,
 * kết hợp <b>Chốt chặn Rò rỉ Dữ liệu Nhạy cảm (Sensitive Data Guard)</b>.
 *
 * <h2>Đặc điểm Kỹ thuật</h2>
 * <ul>
 *   <li><b>Zero-OOM (Không bao giờ đọc toàn bộ tệp):</b> Chỉ đọc tối đa 4 KB đầu tiên của tệp văn bản/mã nguồn.</li>
 *   <li><b>Đọc trực tiếp cấu trúc {@code .docx}, {@code .pptx} và {@code .zip} bằng {@link ZipInputStream}:</b>
 *       Không cần thư viện Apache POI cồng kềnh, vẫn trích xuất được văn bản trong {@code word/document.xml}
 *       hoặc danh sách tệp con bên trong gói {@code .zip}.</li>
 *   <li><b>Sensitive Data Guard:</b> Phát hiện nhanh khóa bí mật ({@code PRIVATE KEY}), token GitHub/OpenAI/Google
 *       hoặc chuỗi mật khẩu trước khi tệp được chia sẻ công khai trong mạng LAN.</li>
 * </ul>
 */
public final class FileContentExtractor {

    /** Kích thước trích xuất tối đa cho Auto-Tagging = 4 KB. */
    public static final int MAX_SNIPPET_BYTES = 4 * 1024;

    private static final Set<String> TEXT_EXTENSIONS = Set.of(
            "txt", "md", "java", "py", "c", "cpp", "h", "hpp", "cs", "js", "ts", "jsx", "tsx",
            "html", "css", "json", "xml", "yml", "yaml", "sql", "csv", "log", "properties",
            "sh", "bat", "ps1", "fxml", "gradle", "toml", "ini"
    );

    private static final Pattern PRIVATE_KEY_PATTERN = Pattern.compile(
            "-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----", Pattern.CASE_INSENSITIVE);
    private static final Pattern API_TOKEN_PATTERN = Pattern.compile(
            "(AIza[0-9A-Za-z\\-_]{30,}|ghp_[0-9A-Za-z]{30,}|sk-[0-9A-Za-z]{30,})");
    private static final Pattern PASSWORD_ASSIGN_PATTERN = Pattern.compile(
            "(password|passwd|pwd|secret_key|api_key|access_token)\\s*[:=]\\s*['\"]?[^\\s'\"]{6,}",
            Pattern.CASE_INSENSITIVE);

    private FileContentExtractor() {
    }

    /**
     * Trích xuất đoạn văn bản đặc trưng (tối đa ~2.000 ký tự) từ một {@link File} trên đĩa.
     */
    public static String extractSnippet(File file) {
        if (file == null || !file.exists() || !file.isFile() || file.length() == 0) {
            return "";
        }
        String ext = getExtension(file.getName());

        try {
            if ("docx".equals(ext)) {
                return extractOpenXmlText(file, "word/document.xml");
            }
            if ("pptx".equals(ext)) {
                return extractOpenXmlText(file, "ppt/slides/slide1.xml");
            }
            if ("zip".equals(ext) || "jar".equals(ext)) {
                try (InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
                    return extractZipDirectoryListing(fis);
                }
            }
            if (TEXT_EXTENSIONS.contains(ext)) {
                byte[] buffer = new byte[(int) Math.min(file.length(), MAX_SNIPPET_BYTES)];
                try (InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
                    int read = fis.read(buffer);
                    if (read > 0) {
                        return cleanReadableText(new String(buffer, 0, read, StandardCharsets.UTF_8), 1_800);
                    }
                }
            }

            // Với PDF hoặc file nhị phân: đọc 4 KB đầu và lọc các chuỗi ASCII có nghĩa
            byte[] buffer = new byte[(int) Math.min(file.length(), MAX_SNIPPET_BYTES)];
            try (InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
                int read = fis.read(buffer);
                if (read > 0) {
                    return extractPrintableAsciiRuns(buffer, read, 900);
                }
            }
        } catch (IOException ignored) {
        }
        return "";
    }

    /**
     * Trích xuất văn bản đọc được từ mảng byte tải thử (ví dụ 8 KB đầu tiên lấy qua {@code DOWNLOAD_CHUNK}).
     */
    public static String extractSnippetFromBytes(String fileName, byte[] rawBytes, int length) {
        if (rawBytes == null || length <= 0) {
            return "";
        }
        int safeLen = Math.min(rawBytes.length, length);
        String ext = getExtension(fileName);

        if ("zip".equals(ext) || "jar".equals(ext) || "docx".equals(ext) || "pptx".equals(ext)) {
            String zipListing = extractZipDirectoryListing(new ByteArrayInputStream(rawBytes, 0, safeLen));
            if (!zipListing.isBlank()) {
                return zipListing;
            }
        }

        if (TEXT_EXTENSIONS.contains(ext)) {
            return cleanReadableText(new String(rawBytes, 0, safeLen, StandardCharsets.UTF_8), 2_200);
        }

        return extractPrintableAsciiRuns(rawBytes, safeLen, 1_200);
    }

    /**
     * Quét nhanh tên tệp và đoạn trích 4 KB đầu để phát hiện nguy cơ rò rỉ khóa bí mật hoặc mật khẩu.
     *
     * @return chuỗi cảnh báo tiếng Việt nếu phát hiện dữ liệu nhạy cảm, hoặc chuỗi rỗng {@code ""} nếu an toàn.
     */
    public static String detectSensitiveWarning(String fileName, String snippet) {
        String lowerName = (fileName == null) ? "" : fileName.toLowerCase(Locale.ROOT);
        if (lowerName.equals(".env") || lowerName.endsWith(".pem") || lowerName.endsWith(".key")
                || lowerName.contains("id_rsa") || lowerName.contains("credentials")) {
            return "Cảnh báo bảo mật: Tệp cấu hình/khóa nhạy cảm (" + fileName + ")";
        }
        if (snippet == null || snippet.isBlank()) {
            return "";
        }
        if (PRIVATE_KEY_PATTERN.matcher(snippet).find()) {
            return "Cảnh báo bảo mật: Phát hiện khóa Private Key (RSA/SSH) bên trong tệp!";
        }
        if (API_TOKEN_PATTERN.matcher(snippet).find()) {
            return "Cảnh báo bảo mật: Phát hiện chuỗi API Token (Google/GitHub/OpenAI) trong tệp!";
        }
        if (PASSWORD_ASSIGN_PATTERN.matcher(snippet).find()) {
            return "Cảnh báo bảo mật: Phát hiện biến mật khẩu/Secret Key dạng rõ trong tệp!";
        }
        return "";
    }

    public static String getExtension(String fileName) {
        if (fileName == null) {
            return "";
        }
        int dot = fileName.lastIndexOf('.');
        if (dot < 0 || dot == fileName.length() - 1) {
            return "";
        }
        return fileName.substring(dot + 1).toLowerCase(Locale.ROOT).trim();
    }

    private static String extractOpenXmlText(File officeFile, String targetEntryPrefix) {
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(officeFile)))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().equalsIgnoreCase(targetEntryPrefix)
                        || entry.getName().startsWith("word/document")
                        || entry.getName().startsWith("ppt/slides/slide")) {
                    byte[] xmlBytes = zis.readNBytes(16 * 1024);
                    String rawXml = new String(xmlBytes, StandardCharsets.UTF_8);
                    // Thay các thẻ kết thúc đoạn bằng khoảng trắng rồi xóa toàn bộ thẻ XML
                    String stripped = rawXml
                            .replaceAll("</w:p>|</a:p>", "\n")
                            .replaceAll("<[^>]+>", " ");
                    return cleanReadableText(stripped, 1_600);
                }
            }
        } catch (IOException ignored) {
        }
        return "";
    }

    private static String extractZipDirectoryListing(InputStream inputStream) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        try (ZipInputStream zis = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null && count < 35) {
                if (!entry.isDirectory()) {
                    if (!sb.isEmpty()) {
                        sb.append(", ");
                    }
                    sb.append(entry.getName());
                    count++;
                }
            }
        } catch (IOException ignored) {
        }
        if (count == 0) {
            return "";
        }
        return "Gói nén chứa các tệp: " + sb;
    }

    private static String cleanReadableText(String text, int maxChars) {
        if (text == null || text.isBlank()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(Math.min(text.length(), maxChars));
        boolean lastWasSpace = false;
        for (int i = 0; i < text.length() && sb.length() < maxChars; i++) {
            char c = text.charAt(i);
            if (c == '\r') {
                continue;
            }
            if (c == '\n' || c == '\t' || Character.isWhitespace(c)) {
                if (!lastWasSpace) {
                    sb.append(c == '\n' ? '\n' : ' ');
                    lastWasSpace = true;
                }
            } else if (!Character.isISOControl(c)) {
                sb.append(c);
                lastWasSpace = false;
            }
        }
        return sb.toString().trim();
    }

    private static String extractPrintableAsciiRuns(byte[] bytes, int length, int maxChars) {
        StringBuilder result = new StringBuilder();
        StringBuilder currentRun = new StringBuilder();
        for (int i = 0; i < length && result.length() < maxChars; i++) {
            int b = bytes[i] & 0xFF;
            if (b >= 32 && b <= 126) {
                currentRun.append((char) b);
            } else {
                if (currentRun.length() >= 5) {
                    if (!result.isEmpty()) {
                        result.append(' ');
                    }
                    result.append(currentRun);
                }
                currentRun.setLength(0);
            }
        }
        if (currentRun.length() >= 5 && result.length() < maxChars) {
            if (!result.isEmpty()) {
                result.append(' ');
            }
            result.append(currentRun);
        }
        return result.toString().trim();
    }
}
