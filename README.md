# Hệ Thống Điều Hành An Ninh Phân Tán - GoLevelDB & gRPC (POLICE DISTRIBUTED DB v6.0)

[![Go Report Card](https://goreportcard.com/badge/github.com/NguyenDucTrung-ndt/goleveldb-distributed-project)](https://goreportcard.com/report/github.com/NguyenDucTrung-ndt/goleveldb-distributed-project)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## ⚙️ Cơ Chế Điều Phối và Lưu Trữ Dữ Liệu (Consistent Hashing)

Hệ thống không phân phối hồ sơ vụ án theo kiểu ngẫu nhiên hay chia đều 50-50, mà sử dụng thuật toán **Consistent Hashing (Băm nhất quán)** để tự động quyết định một vụ án sẽ được lưu trữ tại **Đồn 1** hay **Đồn 2** dựa trên mã **`CaseID`**.

### 📌 Nguyên lý hoạt động chi tiết

1. **Khởi tạo vòng tròn số (Hash Ring):** Hệ thống thiết lập một vòng tròn số logic (giới hạn từ 0 đến 4.294.967.295). Khi khởi động, **Đồn 1** (`Cổng 50051`) và **Đồn 2** (`Cổng 50052`) sẽ tạo ra 10 "nút ảo" (Virtual Nodes) tương ứng cho mỗi đồn, sau đó tự băm và "cắm mốc" cố định tại các vị trí ngẫu nhiên trên vòng tròn này.

2. **Định vị vụ án trên vòng tròn:** Khi bạn bấm nút **Thêm vụ án**, Proxy sẽ lấy mã độc nhất `CaseID` (Ví dụ: `VA_2026_HN001`) đem đi băm thành một con số cụ thể, xác định vị trí của vụ án đó trên vòng tròn số.

3. **Tìm đồn lưu trữ chính (Master Node):** Từ vị trí của vụ án, hệ thống sẽ quét **theo chiều kim đồng hồ** trên vòng tròn. Vụ án di chuyển gặp "nút ảo" của đồn nào đầu tiên, dữ liệu sẽ lập tức được gửi về gRPC Server của đồn đó để ghi xuống cơ sở dữ liệu LevelDB.

> 💡 **Đặc điểm kỹ thuật:**
> * Vì mã băm của một `CaseID` là cố định, nên vụ án đó sẽ **luôn luôn** rơi vào một đồn duy nhất, giúp hệ thống tra cứu chính xác tuyệt đối mà không cần tìm kiếm mù quáng trên toàn bộ các đồn.
> * Để thử nghiệm tính năng phân tán (khiến dữ liệu nhảy từ Đồn 1 sang Đồn 2), bạn chỉ cần **thay đổi ký tự của mã vụ án** khi nhập liệu (Ví dụ thử nghiệm với: `VA01`, `VA02`, `VA03`...). Hàm băm thay đổi sẽ đẩy vụ án sang phân vùng của đồn khác.

---

### 🔄 Cơ chế Sao lưu Đồng thời (Replication)

Để đảm bảo an toàn an ninh dữ liệu khi có sự cố sập nguồn (Fault Tolerance), ngay sau khi ghi thành công vào đồn chính (Master), Proxy sẽ tự động tra cứu bản đồ cấu hình để nhân bản dữ liệu sang đồn dự phòng (Replica) tương ứng:

* Nếu vụ án thuộc về **Đồn 1** (`:50051`) ➡️ Tự động đồng bộ sang **Đồn 1 Dự phòng** (`:50053`).
* Nếu vụ án thuộc về **Đồn 2** (`:50052`) ➡️ Tự động đồng bộ sang **Đồn 2 Dự phòng** (`:50054`).

Nhờ cơ chế này, hồ sơ vụ án luôn được hạ đĩa an toàn tại **2 đồn độc lập** cùng một lúc.

---


## POLICE DISTRIBUTED DB v6.0 
Là hệ thống quản lý và điều hành hồ sơ nghiệp vụ vụ án an ninh được xây dựng trên kiến trúc phân tán. Hệ thống trích xuất dữ liệu thời gian thực từ các phân khu địa cứng local sử dụng **GoLevelDB**, đồng thời đồng bộ và giao tiếp giữa các phân khu (đồn an ninh chính và đồn dự phòng) thông qua giao thức **gRPC** hiệu năng cao, tích hợp giao diện web quản trị trực quan bằng framework **Gin (Golang)**.

---

## 🚀 Tính Năng Cốt Lõi

- **Cơ Sở Dữ Liệu Phân Tán (Distributed Key-Value Store)**: Tổ chức lưu trữ dữ liệu vụ án độc lập tại từng phân khu bằng GoLevelDB, tối ưu tốc độ đọc/ghi dữ liệu thô và tăng cường tính độc lập dữ liệu.
- **Cơ Chế Dự Phòng & Tính Sẵn Sàng Cao (High Availability)**: Thiết lập các cụm Đồn An Ninh đi kèm Đồn Dự Phòng riêng biệt, đảm bảo an toàn dữ liệu, tự động chuyển đổi đồng bộ khi có sự cố phân vùng mạng hoặc sập nút (node failure).
- **Sàng Lọc & Truy Vấn Đa Điều Kiện**: Cho phép cán bộ trích xuất, tìm kiếm hồ sơ vụ án theo mốc thời gian, mã vụ án (Key), tội danh hoặc tên nghi phạm một cách nhanh chóng và chính xác.
- **Phân Quyền Chi Tiết (RBAC)**: Tích hợp hệ thống quản lý tài khoản và phân quyền cán bộ chặt chẽ:
  - *Chỉ huy Cấp cao (Admin)*: Thượng tá (Trưởng Phân Khu), Thiếu tá (Phó Trưởng Đồn) có toàn quyền cấu hình hệ thống, quản lý cán bộ và xuất báo cáo tổng hợp.
  - *Cán bộ nghiệp vụ*: Điều tra viên, trinh sát hình sự thao tác cập nhật hồ sơ theo phạm vi thẩm quyền được giao.
- **Xuất Báo Cáo Nghiệp Vụ**: Hỗ trợ xuất và tải dữ liệu sàng lọc ra định dạng file `.csv` phục vụ công tác báo cáo thủ trưởng cơ quan cấp trên.

---

## 🏗️ Kiến Trúc Phân Khu Dữ Liệu

Hệ thống được thiết kế chia làm các cặp phân khu Master-Slave (Chính - Dự phòng) kết nối chặt chẽ qua gRPC:
- **Cụm Phân Khu 1**: Đồn an ninh số 1 (`db_don_1` tại port `50051`) & Đồn dự phòng số 1 (`db_don_1_du_phong` tại port `50053`).
- **Cụm Phân Khu 2**: Đồn an ninh số 2 (`db_don_2` tại port `50052`) & Đồn dự phòng số 2 (`db_don_2_du_phong` tại port `50054`).

---

## 🛠️ Hướng Dẫn Khởi Chạy Hệ Thống (Dành cho Giảng viên / Người chấm)

> ⚠️ **LƯU Ý TRƯỚC KHI CHẠY (QUAN TRỌNG):**
> - Nếu đây là lần đầu tiên tải mã nguồn về máy hoặc môi trường chưa cài đặt sẵn các gói phụ thuộc, bạn **bắt buộc phải chạy lệnh sau tại thư mục gốc của dự án** để hệ thống tự động tải và đồng bộ các gói thư viện cần thiết (Gin, gRPC, GoLevelDB...):
>   ```bash
>   go mod tidy
>   ```
> - Bạn **không cần tạo trước các thư mục database**. Hệ thống sử dụng GoLevelDB sẽ tự động nhận diện và khởi tạo các phân khu dữ liệu local khi kích hoạt thành công các cổng.

### Bước 1: Khởi chạy 4 Phân khu Đồn An ninh (Mở 4 Terminal riêng biệt)

Kích hoạt các node lưu trữ phân tán backend thông qua các lệnh tương ứng sau:

**Terminal 1 (Đồn an ninh 1 - Node Chính):**
```bash
go run main.go 50051 db_don_1
```
**Terminal 2 (Đồn an ninh 1 - Node Dự phòng):**

```bash
go run main.go 50053 db_don_1_du_phong
```
**Terminal 3 (Đồn an ninh 2 - Node Chính):**
```Bash
go run main.go 50052 db_don_2
```
**Terminal 4 (Đồn an ninh 2 - Node Dự phòng):**
```Bash
go run main.go 50054 db_don_2_du_phong
```
### Bước 2: Khởi chạy Giao diện Web Client (Bảng điều khiển)
Mở một Terminal thứ 5, chạy lệnh dưới đây để kích hoạt HTTP Web Server (sử dụng Gin framework) làm nhiệm vụ kết nối, điều phối dữ liệu từ các phân khu và hiển thị giao diện người dùng:

```Bash
go run web.go
```
💻 Kiểm Thử Hệ Thống & Đánh Giá Nghiệp Vụ
Sau khi kích hoạt thành công cả 5 Terminal, mở trình duyệt web (Chrome, Edge, Safari...) và truy cập vào địa chỉ: http://localhost:8080

1. Tài khoản thử nghiệm (Phân quyền Chỉ Huy Cấp Cao - Admin)
Bạn có thể sử dụng các tài khoản có sẵn trong hệ thống để test tính năng quản trị và phân quyền:

Tài khoản 1: sep2026 (Thượng tá Nguyễn Văn A - Trưởng Phân Khu)

Tài khoản 2: pho_don_01 (Thiếu tá Lê Hoàng Nam - Phó Trưởng Đồn)

Tài khoản 3: chiensi01 (Trung úy Trần Văn B - Cán bộ)

2. Các kịch bản chấm điểm trực quan
Cập nhật hồ sơ vụ án: Thao tác thêm mới hồ sơ vụ án nghiệp vụ (Hệ thống tự động băm key và lưu xuống bộ lưu trữ local GoLevelDB).

Sàng lọc đa điều kiện: Thực hiện tìm kiếm thời gian thực xuyên suốt các phân khu địa cứng theo mã vụ án, mốc thời gian.

Kiểm thử tính phân tán & dự phòng (Fault Tolerance): Thử nghiệm tắt (Stop) 1 trong các đồn chính (ví dụ tắt Terminal 1 của Đồn an ninh 1). Thực hiện lệnh tìm kiếm trên Web, hệ thống vẫn tự động chuyển hướng đọc dữ liệu thông qua đồn dự phòng một cách mượt mà, đảm bảo không gián đoạn dịch vụ.

Xuất báo cáo: Nhấn nút "Tải file báo cáo" để hệ thống tổng hợp dữ liệu sàng lọc và xuất ra định dạng .csv. Hệ thống tích hợp sẵn middleware bảo mật kiểm tra session đăng nhập chặt chẽ (if !isLoggedIn) trước khi cho phép tải file.
