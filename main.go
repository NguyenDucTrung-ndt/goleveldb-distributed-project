package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	// 1. Khởi tạo hoặc Mở cơ sở dữ liệu (tự động tạo thư mục "db_data" nếu chưa có)
	db, err := leveldb.OpenFile("db_data", nil)
	if err != nil {
		log.Fatalf("Không thể mở cơ sở dữ liệu: %v", err)
	}
	// Đảm bảo đóng DB khi hàm main kết thúc để giải phóng bộ nhớ và khóa tệp
	defer db.Close()

	fmt.Println("=== 1. Khởi tạo goleveldb thành công ===")

	// 2. Thao tác GHI dữ liệu (Put)
	err = db.Put([]byte("mssv_01"), []byte("Nguyen Van A"), nil)
	if err != nil {
		log.Fatalf("Lỗi khi ghi dữ liệu: %v", err)
	}
	db.Put([]byte("mssv_02"), []byte("Tran Thi B"), nil)
	db.Put([]byte("mssv_03"), []byte("Le Van C"), nil)
	fmt.Println("-> Đã ghi thành công 3 cặp Key-Value vào DB.")

	// 3. Thao tác ĐỌC dữ liệu (Get)
	value, err := db.Get([]byte("mssv_01"), nil)
	if err != nil {
		log.Printf("Lỗi khi đọc dữ liệu mssv_01: %v", err)
	} else {
		fmt.Printf("-> Đọc dữ liệu thành công! Key 'mssv_01' có Value là: %s\n", string(value))
	}

	// 4. Thao tác DUYỆT dữ liệu (Iterator) - Tự động sắp xếp theo thứ tự Key
	fmt.Println("\n=== 2. Duyệt toàn bộ dữ liệu hiện tại ===")
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		fmt.Printf("   Key: %s | Value: %s\n", string(key), string(val))
	}
	iter.Release() // Giải phóng tài nguyên sau khi duyệt xong
	err = iter.Error()
	if err != nil {
		log.Fatalf("Lỗi trong quá trình duyệt dữ liệu: %v", err)
	}

	// 5. Thao tác XÓA dữ liệu (Delete)
	fmt.Println("\n=== 3. Thử nghiệm Xóa dữ liệu ===")
	err = db.Delete([]byte("mssv_02"), nil)
	if err != nil {
		log.Fatalf("Lỗi khi xóa dữ liệu: %v", err)
	}
	fmt.Println("-> Đã xóa Key 'mssv_02'.")

	// Đọc lại xem còn mssv_02 không
	_, err = db.Get([]byte("mssv_02"), nil)
	if err == leveldb.ErrNotFound {
		fmt.Println("-> Kiểm tra lại: Key 'mssv_02' thực sự không còn tồn tại (ErrNotFound).")
	}
}