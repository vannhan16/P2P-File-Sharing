---
trigger: always_on
---

# Context Dự án: Hệ thống chia sẻ tệp P2P (Peer-to-Peer)

## 1. Tổng quan
- Ứng dụng chia sẻ tệp P2P phi tập trung hoạt động trong mạng nội bộ (LAN).
- Không sử dụng máy chủ trung tâm. Các máy (peer) tự động phát hiện nhau và truyền tệp trực tiếp.

## 2. Định hướng Công nghệ & Sự linh hoạt (Flexible Tech Stack)
- **Nền tảng chính:** Java (JDK 17 hoặc 21).
- **Giao diện:** JavaFX. Khuyến khích tích hợp các thư viện UI hiện đại (như AtlantaFX hoặc JFoenix) để thiết kế giao diện tối giản, sang trọng. Bắt buộc dùng Multithreading (Task/CompletableFuture) để không block UI.
- **Mạng (Networking):** 
  - Khám phá mạng (LAN Discovery) tự động và truyền tải tệp.
  - *Quyền tự quyết của AI:* Được phép và khuyến khích đề xuất sử dụng **Java NIO** (Non-blocking I/O) hoặc **Netty** thay cho Socket TCP/UDP thuần túy truyền thống, miễn là tối ưu được tốc độ truyền file và quản lý luồng dữ liệu tốt hơn.
- **Bảo mật/Toàn vẹn:** Dùng SHA-256 hoặc các thuật toán checksum tối ưu hóa cho file lớn.

## 3. Các tính năng cốt lõi đến nâng cao
- Tự động phát hiện và hiển thị IP/Tên máy các Peer đang online.
- Giao diện chọn file, quản lý thư mục chia sẻ, tìm kiếm file trên mạng.
- Truyền file 1-1, hiển thị thanh tiến trình (Progress Bar) mượt mà và tốc độ truyền (MB/s).
- Cắt nhỏ file (Chunking) để chống tràn RAM khi gửi các file hàng GB.
- Hỗ trợ dừng/tiếp tục tải (Resume) và tải đa nguồn (Swarm/Torrent-like).

## 4. Nguyên tắc Code Chuẩn Kỹ Sư (Clean Code)
- Phân tách kiến trúc chặt chẽ (MVC/MVVM): Tầng UI KHÔNG bao giờ chứa logic xử lý mạng hay I/O.
- Ưu tiên hiệu năng: Xử lý bộ đệm (buffer) thông minh.
- Xử lý triệt để ngoại lệ: Bắt lỗi Firewall, Port đã sử dụng, rớt mạng giữa chừng.
