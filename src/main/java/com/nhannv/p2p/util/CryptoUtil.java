package com.nhannv.p2p.util;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Tiện ích bảo mật mạng P2P:
 * <ul>
 *   <li><b>Xác thực Challenge-Response HMAC-SHA256:</b> Mã PIN không bao giờ truyền dạng thô qua mạng.
 *       Server gửi Nonce ngẫu nhiên, Client tính HMAC-SHA256 với khóa dẫn xuất từ PIN để chứng minh sở hữu PIN.</li>
 *   <li><b>So sánh thời gian không đổi (Constant-time comparison):</b> Chống tấn công kênh bên (Timing Attacks)
 *       bằng {@link MessageDigest#isEqual(byte[], byte[])}.</li>
 *   <li><b>Mã hóa đối xứng AES-256-GCM:</b> Mã hóa nội dung tin nhắn chat và luồng dữ liệu bảo vệ tính
 *       bảo mật (Confidentiality) và tính toàn vẹn (Integrity with 128-bit Authentication Tag).</li>
 *   <li><b>Sinh ngẫu nhiên bảo mật (CSPRNG):</b> Dùng {@link SecureRandom} để sinh Nonce, IV và mã PIN 6 số.</li>
 * </ul>
 */
public final class CryptoUtil {

    private static final String KEY_DERIVATION_ALGO = "PBKDF2WithHmacSHA256";
    private static final String HMAC_ALGO = "HmacSHA256";
    private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final byte[] DEFAULT_SALT = "VKU-P2P-AES256-GCM-SALT-2026".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HMAC_SALT = "VKU-P2P-HMAC-CHALLENGE-SALT-2026".getBytes(StandardCharsets.UTF_8);

    private static final int KEY_LENGTH_BITS = 256;
    private static final int PBKDF2_ITERATIONS = 10_000;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int NONCE_LENGTH_BYTES = 16;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private CryptoUtil() {
    }

    /**
     * Sinh mã PIN bảo mật ngẫu nhiên 6 chữ số bằng CSPRNG {@link SecureRandom}.
     */
    public static String generateSecurePin6() {
        int pinNum = SECURE_RANDOM.nextInt(1_000_000);
        return String.format(Locale.ROOT, "%06d", pinNum);
    }

    /**
     * Sinh một chuỗi Nonce ngẫu nhiên 16 byte (32 ký tự hex) cho giao thức Challenge-Response.
     */
    public static String generateRandomNonce() {
        byte[] bytes = new byte[NONCE_LENGTH_BYTES];
        SECURE_RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    /**
     * Dẫn xuất khóa {@link SecretKeySpec} 256-bit từ mã PIN/Passphrase bằng PBKDF2-HMAC-SHA256.
     * Không chấp nhận PIN rỗng (không sử dụng fallback cố định).
     */
    public static SecretKeySpec deriveAes256Key(String pinOrPassphrase) throws GeneralSecurityException {
        if (pinOrPassphrase == null || pinOrPassphrase.trim().isEmpty()) {
            throw new IllegalArgumentException("Mã PIN hoặc mật khẩu không được rỗng để dẫn xuất khóa AES-256!");
        }
        String effectiveSecret = "VKU-PIN-" + pinOrPassphrase.trim();
        SecretKeyFactory factory = SecretKeyFactory.getInstance(KEY_DERIVATION_ALGO);
        KeySpec spec = new PBEKeySpec(
                effectiveSecret.toCharArray(),
                DEFAULT_SALT,
                PBKDF2_ITERATIONS,
                KEY_LENGTH_BITS
        );
        byte[] keyBytes = factory.generateSecret(spec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }

    /**
     * Dẫn xuất khóa cho HMAC-SHA256 Challenge-Response từ PIN của người dùng.
     */
    public static byte[] deriveHmacKey(String pinOrPassphrase) throws GeneralSecurityException {
        if (pinOrPassphrase == null || pinOrPassphrase.trim().isEmpty()) {
            throw new IllegalArgumentException("Mã PIN không được rỗng để dẫn xuất khóa xác thực HMAC!");
        }
        String effectiveSecret = "VKU-AUTH-" + pinOrPassphrase.trim();
        SecretKeyFactory factory = SecretKeyFactory.getInstance(KEY_DERIVATION_ALGO);
        KeySpec spec = new PBEKeySpec(
                effectiveSecret.toCharArray(),
                HMAC_SALT,
                PBKDF2_ITERATIONS,
                KEY_LENGTH_BITS
        );
        return factory.generateSecret(spec).getEncoded();
    }

    /**
     * Tính toán phản hồi HMAC-SHA256 cho Challenge: {@code HMAC(key=PIN_derived, nonce|command)}.
     */
    public static String createChallengeResponse(String pin, String nonce, String command)
            throws GeneralSecurityException {
        if (pin == null || pin.trim().isEmpty()) {
            return "";
        }
        byte[] hmacKey = deriveHmacKey(pin);
        String message = (nonce == null ? "" : nonce.trim()) + "|" + (command == null ? "" : command.trim());
        Mac mac = Mac.getInstance(HMAC_ALGO);
        mac.init(new SecretKeySpec(hmacKey, HMAC_ALGO));
        byte[] rawHmac = mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(rawHmac);
    }

    /**
     * Xác thực phản hồi Challenge-Response bằng so sánh thời gian không đổi {@link MessageDigest#isEqual}.
     */
    public static boolean verifyChallengeResponse(
            String expectedPin,
            String nonce,
            String command,
            String clientResponseHmac
    ) {
        if (expectedPin == null || expectedPin.trim().isEmpty()) {
            return true; // PIN không bật -> xác thực tự do
        }
        if (clientResponseHmac == null || clientResponseHmac.isBlank()) {
            return false;
        }
        try {
            String expectedHmac = createChallengeResponse(expectedPin, nonce, command);
            byte[] expectedBytes = expectedHmac.getBytes(StandardCharsets.UTF_8);
            byte[] clientBytes = clientResponseHmac.trim().getBytes(StandardCharsets.UTF_8);
            return MessageDigest.isEqual(expectedBytes, clientBytes);
        } catch (GeneralSecurityException e) {
            return false;
        }
    }

    /**
     * Mã hóa một mảng byte bằng AES-256-GCM.
     * Kết quả trả về có cấu trúc: {@code [12-byte IV] + [Ciphertext + 16-byte GCM Tag]}.
     */
    public static byte[] encryptBytes(byte[] plainBytes, int offset, int length, String pin)
            throws GeneralSecurityException {
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        SECURE_RANDOM.nextBytes(iv);

        SecretKeySpec keySpec = deriveAes256Key(pin);
        Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));

        byte[] cipherBytes = cipher.doFinal(plainBytes, offset, length);
        byte[] output = new byte[iv.length + cipherBytes.length];
        System.arraycopy(iv, 0, output, 0, iv.length);
        System.arraycopy(cipherBytes, 0, output, iv.length, cipherBytes.length);
        return output;
    }

    /**
     * Giải mã mảng byte đã được mã hóa bởi {@link #encryptBytes(byte[], int, int, String)}.
     */
    public static byte[] decryptBytes(byte[] ivAndCipherBytes, String pin) throws GeneralSecurityException {
        if (ivAndCipherBytes == null || ivAndCipherBytes.length <= GCM_IV_LENGTH_BYTES) {
            throw new GeneralSecurityException("Dữ liệu mã hóa AES-256-GCM không hợp lệ");
        }
        byte[] iv = Arrays.copyOfRange(ivAndCipherBytes, 0, GCM_IV_LENGTH_BYTES);
        SecretKeySpec keySpec = deriveAes256Key(pin);

        Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
        return cipher.doFinal(
                ivAndCipherBytes,
                GCM_IV_LENGTH_BYTES,
                ivAndCipherBytes.length - GCM_IV_LENGTH_BYTES
        );
    }

    /**
     * Mã hóa chuỗi văn bản UTF-8 sang chuỗi Base64 an toàn bằng AES-256-GCM (dùng cho Chat P2P bảo mật).
     */
    public static String encryptTextToBase64(String plainText, String pin) throws GeneralSecurityException {
        byte[] raw = (plainText == null ? "" : plainText).getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = encryptBytes(raw, 0, raw.length, pin);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * Giải mã chuỗi Base64 về lại văn bản UTF-8 bằng AES-256-GCM.
     */
    public static String decryptTextFromBase64(String base64Cipher, String pin) throws GeneralSecurityException {
        byte[] decoded = Base64.getDecoder().decode(base64Cipher);
        byte[] plain = decryptBytes(decoded, pin);
        return new String(plain, StandardCharsets.UTF_8);
    }

    public static String sha256Hex(byte[] data) {
        if (data == null) {
            return "";
        }
        MessageDigest md = HashUtil.newSha256Digest();
        return HashUtil.toHexString(md.digest(data));
    }

    public static String calculateFileSha256(File file) throws IOException {
        return HashUtil.calculateSHA256(file);
    }
}
