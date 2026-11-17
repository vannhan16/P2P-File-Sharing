package com.nhannv.p2p.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;

/**
 * Tiện ích mã hóa đầu-cuối <b>AES-256-GCM</b> (Milestone 6 - Gói 1: Bảo mật Đường truyền).
 *
 * <h2>Giải thích Cơ chế Mã hóa AES-256-GCM cho Buổi Bảo vệ Đồ án</h2>
 * <ul>
 *   <li><b>Dẫn xuất khóa mật mã (Key Derivation - PBKDF2):</b>
 *       Từ mã PIN 4 chữ số của người dùng (hoặc khóa phiên mặc định của mạng P2P),
 *       hệ thống dùng thuật toán {@code PBKDF2WithHmacSHA256} với 10.000 vòng lặp để sinh ra
 *       khóa đối xứng <b>AES 256-bit</b>.</li>
 *   <li><b>Chế độ {@code AES/GCM/NoPadding} (Authenticated Encryption):</b>
 *       Khác với AES-CBC truyền thống, chế độ <b>GCM (Galois/Counter Mode)</b> vừa mã hóa dữ liệu
 *       chống nghe lén (Wireshark Sniffing), vừa đính kèm nhãn xác thực 128-bit (Authentication Tag)
 *       để phát hiện mọi hành vi sửa đổi gói tin trên đường truyền.</li>
 *   <li><b>Vector khởi tạo ngẫu nhiên (12-byte IV):</b>
 *       Mỗi khối dữ liệu hoặc tin nhắn đều sinh một IV ngẫu nhiên 12 byte bằng {@link SecureRandom},
 *       đảm bảo cùng một nội dung nhưng mỗi lần gửi sẽ tạo ra bản mã hoàn toàn khác nhau.</li>
 * </ul>
 */
public final class CryptoUtil {

    private static final String KEY_DERIVATION_ALGO = "PBKDF2WithHmacSHA256";
    private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final byte[] DEFAULT_SALT = "VKU-P2P-AES256-GCM-SALT-2026".getBytes(StandardCharsets.UTF_8);
    private static final String DEFAULT_FALLBACK_PASSPHRASE = "VKU-P2P-DEFAULT-SESSION-KEY";

    private static final int KEY_LENGTH_BITS = 256;
    private static final int PBKDF2_ITERATIONS = 10_000;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private CryptoUtil() {
    }

    /**
     * Dẫn xuất khóa {@link SecretKeySpec} 256-bit từ mã PIN (hoặc dùng khóa phiên mặc định nếu không đặt PIN).
     */
    public static SecretKeySpec deriveAes256Key(String pinOrPassphrase) throws GeneralSecurityException {
        String effectiveSecret = (pinOrPassphrase == null || pinOrPassphrase.isBlank())
                ? DEFAULT_FALLBACK_PASSPHRASE
                : ("VKU-PIN-" + pinOrPassphrase.trim());
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
     * Mã hóa một mảng byte bằng AES-256-GCM. Kết quả trả về có cấu trúc: {@code [12-byte IV] + [Ciphertext + 16-byte GCM Tag]}.
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
}
