---
name: github-auto-commit
description: Tự động phân tích git diff, sinh commit message theo chuẩn Conventional Commits và push code lên remote repository. Kích hoạt khi người dùng gõ /github-auto-commit, hoặc yêu cầu "lưu code", "đẩy code", "push code".
---

# GitHub Auto Commit & Push

## Summary
Kỹ năng này biến Antigravity thành một Git Master, tự động hóa hoàn toàn quy trình lưu trữ phiên bản mã nguồn, đảm bảo lịch sử commit luôn sạch sẽ, chuyên nghiệp và dễ đọc cho làm việc nhóm.

## Workflow (Các bước thực thi nghiêm ngặt)

### 1. Kiểm tra trạng thái (Pre-check)
- Chạy `git status` để lấy danh sách các file Untracked, Modified, Deleted.
- Nếu không có file nào thay đổi, thông báo: "Không có thay đổi nào để commit" và dừng lại.

### 2. Phân tích mã nguồn (Diff Analysis)
- Chạy `git diff` và `git diff --cached` để đọc chi tiết các dòng code được thêm/sửa/xóa.
- Hiểu bối cảnh của thay đổi (ví dụ: đang sửa UI JavaFX, hay đang thêm luồng Socket TCP).

### 3. Sinh Commit Message (Conventional Commits)
Dựa vào phân tích, tự động tạo Commit Message gồm Title và Body:

**Cấu trúc Title:** `<type>(<scope>): <short description>`

- `feat`: Tính năng mới (vd: module TCP).
- `fix`: Sửa lỗi (vd: lỗi đơ UI).
- `refactor`: Viết lại code nhưng không đổi logic.
- `style`: Định dạng code, format UI FXML.
- `docs`: Cập nhật tài liệu, README.

**Cấu trúc Body (Nêu rõ Lý do):** Ghi chú ngắn gọn tại sao lại có sự thay đổi này (vd: "Thêm luồng Task để tránh block JavaFX Application Thread").

### 4. Thực thi Git Commands
Tiến hành chạy tuần tự qua Terminal:

1. `git add .`
2. `git commit -m "<Title>" -m "<Body>"`
3. Lấy tên nhánh hiện tại (thường là `master` hoặc `main`).
4. `git push origin <tên-nhánh>`

### 5. Báo cáo kết quả
- In ra màn hình mã hash của commit và nội dung commit message vừa tạo.
- Chúc mừng người dùng đã hoàn thành một chặng code.

## Gotchas & Safety Rules (Các lỗi tuyệt đối tránh)
- Tuyệt đối **KHÔNG** sử dụng `git push --force` dưới bất kỳ hình thức nào.
- Nếu quá trình push bị từ chối do Conflict (xung đột mã nguồn với người khác) hoặc chưa pull code mới, hãy **DỪNG LẠI** ngay. Báo cáo lỗi cho người dùng và đề xuất chạy `git pull --rebase`.
- Kiểm tra lướt qua xem có file nào dung lượng quá lớn hoặc thư mục rác (`target/`, `.idea/`) đang bị add nhầm không. Nếu có, nhắc người dùng cập nhật `.gitignore` trước khi commit.
