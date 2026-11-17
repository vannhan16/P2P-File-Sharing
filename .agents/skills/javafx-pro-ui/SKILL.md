---
name: javafx-pro-ui
description: Thiết kế và tối ưu hóa giao diện JavaFX đạt chuẩn phần mềm Desktop thương mại (phong cách Linear, LocalSend, macOS/Fluent UI), loại bỏ hoàn toàn dấu vết AI code. Kích hoạt khi người dùng gọi /javafx-pro-ui hoặc yêu cầu làm đẹp, thiết kế lại, tối ưu UI/UX.
---
# JavaFX Crafted Desktop UI Skill

## Summary
Kỹ năng này áp dụng hệ thống thiết kế (Design System) cấp độ Production cho ứng dụng JavaFX, thay thế hoàn toàn các mẫu giao diện thô cứng hoặc lòe loẹt thường thấy khi AI sinh code tự động.

## 1. Anti-AI-Slop Rules (Những điều CẤM KỴ tuyệt đối)
- **KHÔNG dùng Emoji trong nút bấm hoặc nhãn** (Cấm dùng 📁, 🚀, ✅, 🔍). Bắt buộc dùng thư viện Vector Icon (`org.kordamp.ikonli:ikonli-javafx` với bộ `ikonli-feather-pack` hoặc `ikonli-materialdesign2-pack`) hoặc SVGPath sắc nét.
- **KHÔNG dùng màu cơ bản nguyên bản (Pure RGB)** như `red`, `blue`, `green`, `#0000FF` hay gradient tím-hồng kiểu web AI.
- **KHÔNG dùng inline style rời rạc (`style="..."`)** tràn lan trong FXML. Toàn bộ style phải gom vào file `src/main/resources/com/nhannv/p2p/css/app-theme.css` với hệ thống class rõ ràng.
- **KHÔNG để bảng trống trơn** với dòng chữ mặc định `"No content in table"`. Phải thiết kế `placeholder` tinh tế có icon mờ và dòng hướng dẫn phụ.
- **KHÔNG để các nút bấm to bằng nhau và cùng màu**. Phải phân cấp rõ: 1 nút Primary (`accent`), các nút còn lại là Secondary (`button-outlined`) hoặc Ghost (`flat`).

## 2. Hệ thống Màu sắc & Phân lớp bề mặt (3-Tier Surface Palette)
Sử dụng nền tảng **AtlantaFX** (`PrimerDark` hoặc `CupertinoDark` / `PrimerLight`) kết hợp tinh chỉnh CSS tại `app-theme.css`:
- **Surface 0 (App Background):** `#0D1117` (Dark) hoặc `#F8FAFC` (Light) - Nền tổng thể.
- **Surface 1 (Sidebar / Panels / Cards):** `#161B22` (Dark) hoặc `#FFFFFF` (Light) - Có viền mảnh `1px solid` màu `#30363D` (Dark) / `#E2E8F0` (Light).
- **Surface 2 (Hover / Selected / Input Field):** `#21262D` (Dark) hoặc `#F1F5F9` (Light).
- **Accent Color (Điểm nhấn duy nhất):** Indigo/Blue chuẩn kỹ thuật (`#3B82F6` hoặc `#2563EB`).
- **Semantic Status Colors (Dạng Pill/Badge nền mờ 15%, chữ đậm):**
  + Online / SHA-256 Verified: Text `#10B981`, Background `rgba(16, 185, 129, 0.12)`.
  + Transferring / Syncing: Text `#3B82F6`, Background `rgba(59, 130, 246, 0.12)`.
  + Paused / Warning: Text `#F59E0B`, Background `rgba(245, 158, 11, 0.12)`.
  + Error / Hash Mismatch: Text `#EF4444`, Background `rgba(239, 68, 68, 0.12)`.

## 3. Hệ thống Typography & Hiển thị số liệu kỹ thuật
- **Font chính (UI Text):** `'Inter'`, `'Segoe UI Variable'`, `'Segoe UI'`, system-ui.
- **Font kỹ thuật (BẮT BUỘC dùng Monospace):** Dùng `'JetBrains Mono'`, `'Cascadia Code'`, `'Consolas'` cho toàn bộ các thông số mạng:
  + Địa chỉ IP & Port (`192.168.1.15:8889`, `26.145.88.12`).
  + Mã băm SHA-256 (hiển thị dạng rút gọn `a8f5...92c1` kèm Tooltip hiện full 64 ký tự và nút copy nhanh).
  + Tốc độ truyền (`42.8 MB/s`), dung lượng tệp (`1.24 GB`) và phần trăm tiến độ (`78.4%`) để các con số không bị nhảy giật chiều ngang (tabular-nums) khi cập nhật theo thời gian thực.

## 4. Bố cục chuẩn Ứng dụng Mạng Chuyên nghiệp (Layout Architecture)
Thiết kế theo cấu trúc 3 vùng cân đối với quy tắc lưới 8pt (`padding: 16px`, `spacing: 12px`, `border-radius: 8px`):
1. **Top Header Bar (Cao 52px):**
   - Bên trái: Tên ứng dụng gọn gàng + trạng thái Server.
   - Bên phải: Các "Network Identity Pills" hiển thị rõ `LAN IP: 192.168.x.x`, `Radmin VPN: 26.x.x.x`, trạng thái `PIN: ON/OFF` và nút chuyển đổi giao diện Sáng/Tối (Theme Toggle).
2. **Left Sidebar - Radar Thiết bị (Rộng 260px):**
   - Ô nhập kết nối nhanh (`IP:Port` / Radmin IP) tích hợp nút kết nối gọn trên cùng một hàng (`InputGroup`).
   - Danh sách Peers sử dụng **Custom `ListCell<Peer>`** (tuyệt đối không dùng `toString()` mặc định): Mỗi dòng là một Card nhỏ gồm chấm tròn xanh lá (`Circle` 8px) báo Online, Tên máy in đậm (13px), địa chỉ IP in mờ font Monospace (11px) ở dòng dưới.
3. **Center Workspace - Không gian Quản lý Tệp & Telemetry:**
   - Thanh công cụ (Toolbar) phẳng: Ô tìm kiếm có icon kính lúp bên trái, bên phải là các nút chọn Shared Folder, Làm mới, Cấu hình Firewall.
   - Bảng `TableView` tinh chỉnh CSS: Ẩn đường kẻ dọc thô kệch, chỉ giữ đường kẻ ngang mảnh, chiều cao dòng cố định `40px`, căn phải cột Dung lượng, hiển thị cột SHA-256 dạng thẻ code nhỏ.
   - **Bottom Transfer Dock (Khu vực Tiến trình & Đồ thị):** Thiết kế như một bảng điều khiển âm thanh/tải xuống cố định ở đáy, bên trái là thông tin file đang tải + thanh `ProgressBar` mỏng hiện đại (cao 6px, bo góc tròn), ở giữa là đồ thị `AreaChart` mượt mà (ẩn lưới trục rườm rà), bên phải là cụm nút điều khiển `Pause` / `Resume` / `Cancel` dùng icon chuẩn.

## 5. Quy trình Thực thi khi kích hoạt Skill
1. Kiểm tra `pom.xml`, bổ sung `atlantafx-base` và `ikonli-javafx` + `ikonli-feather-pack` nếu chưa có.
2. Tạo/cập nhật file `src/main/resources/com/nhannv/p2p/css/app-theme.css`.
3. Viết Custom `ListCell` cho danh sách Peer và Custom `TableCell` cho bảng tệp (SHA-256 badge, kích thước file).
4. Cập nhật `MainView.fxml` và `MainController.java` áp dụng các class CSS.
5. Chạy `mvn clean compile` và `mvn javafx:run` để kiểm tra giao diện thực tế.
