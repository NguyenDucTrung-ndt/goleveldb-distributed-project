# Hệ Thống Điều Hành An Ninh Phân Tán - GoLevelDB & gRPC

## Hướng dẫn khởi chạy hệ thống (Dành cho Giảng viên/Người chấm)

Khi tải mã nguồn về lần đầu, bạn không cần tạo trước các thư mục database. Hệ thống sử dụng GoLevelDB sẽ tự động khởi tạo phân khu dữ liệu khi kích hoạt các cổng.

### Bước 1: Khởi chạy 4 Phân khu Đồn An ninh (Mở 4 Terminal riêng biệt)
```bash
go run main.go 50051 db_don_1
go run main.go 50053 db_don_1_du_phong
go run main.go 50052 db_don_2
go run main.go 50054 db_don_2_du_phong