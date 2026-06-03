package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "goleveldb-demo/storage"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type UserAccount struct {
	Password string `json:"password"`
	Role     string `json:"role"`
	FullName string `json:"full_name"`
}

const (
	roleChief  = "chief"
	roleDeputy = "deputy"
	roleViewer = "viewer"
)

type CaseSummary struct {
	CaseID        string `json:"case_id"`
	OccurredAt    string `json:"occurred_at"`
	Title         string `json:"title"`
	Suspect       string `json:"suspect"`
	Description   string `json:"description"`
	AutopsyReport string `json:"autopsy_report"`
	EvidenceList  string `json:"evidence_list"`
}

type PoliceHashRing struct {
	vnodes       int
	ring         map[uint32]string
	sortedHashes []uint32
}

func NewPoliceHashRing(vnodes int) *PoliceHashRing {
	return &PoliceHashRing{vnodes: vnodes, ring: make(map[uint32]string)}
}

func (h *PoliceHashRing) AddStation(stationAddr string) {
	for i := 0; i < h.vnodes; i++ {
		vnodeKey := stationAddr + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))
		h.ring[hash] = stationAddr
		h.sortedHashes = append(h.sortedHashes, hash)
	}
	sort.Slice(h.sortedHashes, func(i, j int) bool { return h.sortedHashes[i] < h.sortedHashes[j] })
}

func (h *PoliceHashRing) RouteCase(caseID string) string {
	if len(h.sortedHashes) == 0 {
		return ""
	}
	hash := crc32.ChecksumIEEE([]byte(caseID))
	idx := sort.Search(len(h.sortedHashes), func(i int) bool { return h.sortedHashes[i] >= hash })
	if idx == len(h.sortedHashes) {
		idx = 0
	}
	return h.ring[h.sortedHashes[idx]]
}

func checkNodeStatus(addr string) string {
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(200*time.Millisecond),
	)
	if err != nil {
		return "OFFLINE"
	}
	defer conn.Close()
	return "ONLINE"
}

var replicationMap = map[string]string{
	"127.0.0.1:50051": "127.0.0.1:50053",
	"127.0.0.1:50052": "127.0.0.1:50054",
}

var userDB = map[string]UserAccount{
	"sep2026":     {Password: "matkhausep", Role: roleChief, FullName: "Thượng tá Nguyễn Văn A (Trưởng Đồn)"},
	"pho_don_01":  {Password: "matkhaupho", Role: roleDeputy, FullName: "Thiếu tá Lê Hoàng Nam (Phó Trưởng Đồn)"},
	"chiensi01":   {Password: "matkhau01", Role: roleViewer, FullName: "Trung úy Trần Văn B (Trinh sát Hình sự)"},
	"trinhsat_02": {Password: "matkhau02", Role: roleViewer, FullName: "Đại úy Phạm Minh Hải (Trinh sát Địa bàn)"},
	"phap_y_03":   {Password: "matkhau03", Role: roleViewer, FullName: "Thượng úy Nguyễn Thị Thu (Chuyên viên Pháp y)"},
}

var globalCases = []CaseSummary{
	{
		CaseID:        "VA_2026_HN001",
		OccurredAt:    "2026-05-15 22:30",
		Title:         "Giết người cướp tài sản công vụ",
		Suspect:       "Nguyễn Văn Hoành",
		Description:   "Đối tượng Hoành đột nhập vào trạm gác lúc nửa đêm, dùng hung khí tấn công chiến sĩ trực ban từ phía sau nhằm lấy đi tài liệu tối mật.",
		AutopsyReport: "Nạn nhân tử vong do mất máu cấp. Vết thương chí mạng sâu 5cm ở vùng gáy do vật sắc nhọn gây ra.",
		EvidenceList:  "01 Cây dao bấm kim loại màu đen có dính vết máu; 01 Xe máy tháo biển số.",
	},
}

var currentRole = ""
var currentFullName = ""
var isLoggedIn = false

func canAddCase(role string) bool {
	return role == roleChief || role == roleDeputy
}

func canManageUsers(role string) bool {
	return role == roleChief
}

func putCaseToStation(addr, caseID, caseDataJSON string) (bool, string) {
	if addr == "" {
		return false, "Không tìm thấy đồn dự phòng."
	}
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return false, fmt.Sprintf("Đồn %s offline.", addr)
	}
	defer conn.Close()
	callCtx, cancelCall := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancelCall()
	client := pb.NewPoliceStorageServiceClient(conn)
	resp, err := client.PutCase(callCtx, &pb.CaseRequest{CaseId: caseID, CaseDataJson: caseDataJSON})
	if err != nil {
		return false, fmt.Sprintf("Đồn %s không ghi được: %v", addr, err)
	}
	if !resp.Success {
		return false, fmt.Sprintf("Đồn %s từ chối ghi: %s", addr, resp.Message)
	}
	return true, fmt.Sprintf("Đồn %s đã ghi thành công.", addr)
}

func saveCasesToFile() {
	data, _ := json.MarshalIndent(globalCases, "", "  ")
	_ = ioutil.WriteFile("global_cases_cache.json", data, 0644)
}

func loadCasesFromFile() {
	data, err := ioutil.ReadFile("global_cases_cache.json")
	if err == nil {
		_ = json.Unmarshal(data, &globalCases)
	}
}

func main() {
	loadCasesFromFile()
	ring := NewPoliceHashRing(10)
	ring.AddStation("127.0.0.1:50051") // Đồn 1
	ring.AddStation("127.0.0.1:50052") // Đồn 2
	r := gin.Default()
	_ = r.SetTrustedProxies(nil)
	r.LoadHTMLGlob("templates/*")

	r.GET("/", func(c *gin.Context) {
		sort.Slice(globalCases, func(i, j int) bool { return globalCases[i].OccurredAt > globalCases[j].OccurredAt })
		msg := c.Query("msg")
		status1 := checkNodeStatus("127.0.0.1:50051")
		status1Rep := checkNodeStatus("127.0.0.1:50053")
		status2 := checkNodeStatus("127.0.0.1:50052")
		status2Rep := checkNodeStatus("127.0.0.1:50054")
		c.HTML(http.StatusOK, "index.html", gin.H{
			"LoggedIn":       isLoggedIn,
			"Role":           currentRole,
			"FullName":       currentFullName,
			"CanAddCase":     canAddCase(currentRole),
			"CanManageUsers": canManageUsers(currentRole),
			"UserList":       userDB,
			"RecentCases":    globalCases,
			"SuccessMsg":     msg,
			"Status1":        status1,
			"Status1Rep":     status1Rep,
			"Status2":        status2,
			"Status2Rep":     status2Rep,
		})
	})

	r.POST("/login", func(c *gin.Context) {
		username, password := c.PostForm("username"), c.PostForm("password")
		user, exists := userDB[username]
		if exists && user.Password == password {
			isLoggedIn, currentRole, currentFullName = true, user.Role, user.FullName
			c.Redirect(http.StatusSeeOther, "/")
		} else {
			c.HTML(http.StatusOK, "index.html", gin.H{"LoggedIn": false, "Error": "Sai mật mã an ninh!"})
		}
	})

	r.GET("/logout", func(c *gin.Context) {
		isLoggedIn = false
		c.Redirect(http.StatusSeeOther, "/")
	})

	r.POST("/user/add", func(c *gin.Context) {
		if !isLoggedIn || !canManageUsers(currentRole) {
			c.String(http.StatusForbidden, "Truy cập bị chặn. Chỉ Trưởng đồn mới có quyền này.")
			return
		}
		newUsername := strings.TrimSpace(c.PostForm("username"))
		newPassword := c.PostForm("password")
		newRole := c.PostForm("role")
		newFullName := strings.TrimSpace(c.PostForm("full_name"))

		if newUsername == "" || newPassword == "" || newFullName == "" {
			c.Redirect(http.StatusSeeOther, "/?msg="+uint32ToString(crc32.ChecksumIEEE([]byte("Vui lòng điền đủ thông tin!"))))
			return
		}
		if _, exists := userDB[newUsername]; exists {
			c.Redirect(http.StatusSeeOther, "/?msg=Tai khoan da ton tai!")
			return
		}

		userDB[newUsername] = UserAccount{
			Password: newPassword,
			Role:     newRole,
			FullName: newFullName,
		}
		c.Redirect(http.StatusSeeOther, "/?msg=Them can bo thanh cong!")
	})

	r.POST("/user/delete", func(c *gin.Context) {
		if !isLoggedIn || !canManageUsers(currentRole) {
			c.String(http.StatusForbidden, "Truy cập bị chặn. Chỉ Trưởng đồn mới có quyền này.")
			return
		}
		targetUsername := c.PostForm("username")
		if targetUsername == "sep2026" {
			c.Redirect(http.StatusSeeOther, "/?msg=Khong the xoa tai khoan Truong don!")
			return
		}
		if _, exists := userDB[targetUsername]; !exists {
			c.Redirect(http.StatusSeeOther, "/?msg=Tai khoan khong ton tai!")
			return
		}

		delete(userDB, targetUsername)
		c.Redirect(http.StatusSeeOther, "/?msg=Xoa can bo thanh cong!")
	})

	r.POST("/add", func(c *gin.Context) {
		if !isLoggedIn || !canAddCase(currentRole) {
			c.String(http.StatusForbidden, "Truy cập bị chặn.")
			return
		}
		caseID := c.PostForm("case_id")
		title := c.PostForm("title")
		suspect := c.PostForm("suspect")
		occurredAt := strings.Replace(c.PostForm("occurred_at"), "T", " ", 1)
		description := c.PostForm("description")
		autopsyReport := c.PostForm("autopsy_report")
		evidenceList := c.PostForm("evidence_list")
		caseDataJSON := fmt.Sprintf(`{"title": "%s", "suspect": "%s", "status": "Under_Investigation", "occurred_at": "%s", "description": "%s", "autopsy_report": "%s", "evidence_list": "%s"}`,
			title, suspect, occurredAt, description, autopsyReport, evidenceList)
		targetMaster := ring.RouteCase(caseID)
		targetReplica := replicationMap[targetMaster]
		masterOK, masterMsg := putCaseToStation(targetMaster, caseID, caseDataJSON)
		replicaOK, replicaMsg := putCaseToStation(targetReplica, caseID, caseDataJSON)
		if !masterOK && !replicaOK {
			c.String(http.StatusServiceUnavailable, "Không thể thêm hồ sơ: tất cả đồn lưu trữ đang tắt hoặc không phản hồi. %s %s", masterMsg, replicaMsg)
			return
		}
		msgLog := masterMsg + " " + replicaMsg
		globalCases = append(globalCases, CaseSummary{
			CaseID:        caseID,
			OccurredAt:    occurredAt,
			Title:         title,
			Suspect:       suspect,
			Description:   description,
			AutopsyReport: autopsyReport,
			EvidenceList:  evidenceList,
		})
		saveCasesToFile()
		c.Redirect(http.StatusSeeOther, "/?msg="+msgLog)
	})

	r.GET("/search", func(c *gin.Context) {
		startTime := c.Query("start_time")
		endTime := c.Query("end_time")
		searchTitle := strings.ToLower(strings.TrimSpace(c.Query("search_title")))
		if startTime != "" {
			startTime = strings.Replace(startTime, "T", " ", 1)
		}
		if endTime != "" {
			endTime = strings.Replace(endTime, "T", " ", 1)
		}
		var filteredCases []CaseSummary
		for _, v := range globalCases {
			match := true
			if startTime != "" && v.OccurredAt < startTime {
				match = false
			}
			if endTime != "" && v.OccurredAt > endTime {
				match = false
			}
			if searchTitle != "" {
				inTitle := strings.Contains(strings.ToLower(v.Title), searchTitle)
				inSuspect := strings.Contains(strings.ToLower(v.Suspect), searchTitle)
				inDesc := strings.Contains(strings.ToLower(v.Description), searchTitle)
				if !inTitle && !inSuspect && !inDesc {
					match = false
				}
			}
			if match {
				filteredCases = append(filteredCases, v)
			}
		}
		c.HTML(http.StatusOK, "search.html", gin.H{
			"LoggedIn":       isLoggedIn,
			"Role":           currentRole,
			"FullName":       currentFullName,
			"CanAddCase":     canAddCase(currentRole),
			"CanManageUsers": canManageUsers(currentRole),
			"RecentCases":    filteredCases,
			"SuccessMsg":     fmt.Sprintf("Hệ thống tìm thấy %d kết quả phù hợp nghiệp vụ.", len(filteredCases)),
			"StartTime":      c.Query("start_time"),
			"EndTime":        c.Query("end_time"),
			"SearchTitle":    c.Query("search_title"),
		})
	})

	// =========================================================================
	// SỬA LỖI 404 VÀ THỰC HIỆN PHÂN QUYỀN TRUY CẬP TỪNG ĐỒN (DYNAMIC ROUTING)
	// =========================================================================
	r.GET("/node/:port", func(c *gin.Context) {
		// 1. KIỂM TRA ĐĂNG NHẬP VÀ PHÂN QUYỀN (Chỉ cho Chief và Deputy vào)
		if !isLoggedIn || (currentRole != roleChief && currentRole != roleDeputy) {
			c.HTML(http.StatusForbidden, "index.html", gin.H{
				"LoggedIn": isLoggedIn,
				"Role":     currentRole,
				"FullName": currentFullName,
				"Error":    "🚨 TRUY CẬP BỊ TỪ CHỐI: Chỉ cấp chỉ huy (Trưởng đồn/Phó đồn) mới có quyền truy cập trực tiếp đồn!",
			})
			return
		}

		// 2. LẤY THÔNG SỐ CỔNG (PORT) TỪ ĐƯỜNG DẪN URL
		port := c.Param("port")

		// Xác định thông tin của Node dựa trên Port để hiển thị động lên giao diện
		stationName := "ĐỒN AN NINH TRUNG TÂM"
		mainAddr := "127.0.0.1:" + port
		backupAddr := "N/A (Chưa cấu hình Node dự phòng)"
		currentStationIdx := "1"

		if port == "50051" || port == "50053" {
			stationName = "ĐỒN CẢNH SÁT SỐ 1 - KHU VỰC NỘI THÀNH"
			mainAddr = "127.0.0.1:50051"
			backupAddr = "127.0.0.1:50053"
			currentStationIdx = "1"
		} else if port == "50052" || port == "50054" {
			stationName = "ĐỒN CẢNH SÁT SỐ 2 - KHU VỰC NGOẠI Ô"
			mainAddr = "127.0.0.1:50052"
			backupAddr = "127.0.0.1:50054"
			currentStationIdx = "2"
		}

		// 3. LỌC DỮ LIỆU THỰC TẾ TRÊN HASH RING THUỘC VỀ ĐỒN NÀY
		searchTitle := strings.ToLower(strings.TrimSpace(c.Query("search_title")))
		var stationCases []CaseSummary

		for _, v := range globalCases {
			assignedStation := ring.RouteCase(v.CaseID)
			// Nếu vụ án được định tuyến chính xác vào Main Node tương ứng
			if assignedStation == mainAddr {
				if searchTitle == "" || strings.Contains(strings.ToLower(v.Title), searchTitle) || strings.Contains(strings.ToLower(v.Suspect), searchTitle) {
					stationCases = append(stationCases, v)
				}
			}
		}

		// 4. KIỂM TRA TRẠNG THÁI VẬT LÝ THỜI GIAN THỰC
		status := checkNodeStatus(mainAddr)
		statusRep := checkNodeStatus(backupAddr)

		// 5. TRẢ VỀ TRANG CHI TIẾT ĐỒN MẠNG KHÔNG BỊ LỖI 404
		c.HTML(http.StatusOK, "station_detail.html", gin.H{
			"LoggedIn":       isLoggedIn,
			"Role":           currentRole,
			"FullName":       currentFullName,
			"StationName":    stationName + " (Cổng mạng: " + port + ")",
			"MainAddr":       mainAddr,
			"BackupAddr":     backupAddr,
			"Status":         status,
			"StatusRep":      statusRep,
			"RecentCases":    stationCases,
			"SearchTitle":    c.Query("search_title"),
			"CurrentStation": currentStationIdx,
		})
	})

	r.GET("/export", func(c *gin.Context) {
		c.Header("Content-Description", "File Transfer")
		c.Header("Content-Disposition", "attachment; filename=Bao_Cao_Ho_So_Vu_An.csv")
		c.Header("Content-Type", "text/csv; charset=utf-8")
		c.Writer.Write([]byte{0xEF, 0xBB, 0xBF})
		csvContent := "Mã Vụ Án,Ngày Xảy Ra,Tội Danh/Tiêu Đề,Đối Tượng Nghi Vấn,Mô Tả Diễn Biến,Báo Cáo Pháp Y,Vật Chứng Tịch Thu\n"
		for _, v := range globalCases {
			cleanDesc := strings.ReplaceAll(strings.ReplaceAll(v.Description, "\n", " "), ",", ";")
			cleanAutopsy := strings.ReplaceAll(strings.ReplaceAll(v.AutopsyReport, "\n", " "), ",", ";")
			cleanEvidence := strings.ReplaceAll(strings.ReplaceAll(v.EvidenceList, "\n", " "), ",", ";")
			cleanTitle := strings.ReplaceAll(v.Title, ",", ";")
			cleanSuspect := strings.ReplaceAll(v.Suspect, ",", ";")
			csvContent += fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
				v.CaseID, v.OccurredAt, cleanTitle, cleanSuspect, cleanDesc, cleanAutopsy, cleanEvidence)
		}
		c.String(http.StatusOK, csvContent)
	})

	r.Run("localhost:8080")
}

func uint32ToString(n uint32) string {
	return strconv.FormatUint(uint64(n), 10)
}
