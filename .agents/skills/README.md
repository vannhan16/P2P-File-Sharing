# Hệ thống Agent Skills (Project-scoped)

Thư mục này chứa các kỹ năng (Skills) dành riêng cho dự án P2P-File-Sharing. Antigravity sẽ tự động nạp các kỹ năng này khi mở dự án.

## Cách tạo Skill mới
1. Tạo một thư mục con mới: `.agents/skills/<ten-skill-viet-thuong-co-gach-noi>/`
2. Tạo file `SKILL.md` bên trong thư mục đó.
3. Cấu trúc bắt buộc của một file `SKILL.md`:
   ```yaml
   ---
   name: ten-skill
   description: Mô tả chi tiết khi nào Antigravity nên tự động kích hoạt skill này.
   ---
   # Tên Skill
   ## Mục tiêu
   ## Các bước thực hiện
   ## Lưu ý (Gotchas)
   ```
