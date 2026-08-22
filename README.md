# Thu thập dữ liệu giao thông bằng GitHub Actions

Bộ tệp này chạy Python crawler tự động mỗi 10 phút và lưu kết quả vào:

`data/traffic_congestion.csv`

Không cần Google Cloud, Docker, thẻ thanh toán hoặc để máy tính cá nhân hoạt động.

## 1. Tạo repository

1. Đăng nhập GitHub và chọn **New repository**.
2. Đặt tên, ví dụ: `hcmc-traffic-congestion-data`.
3. Chọn **Public** để GitHub Actions dùng runner tiêu chuẩn miễn phí.
4. Chọn **Create repository**.

## 2. Đưa các tệp lên GitHub

Repository phải có đúng cấu trúc sau:

```text
hcmc-traffic-congestion-data/
├── .github/
│   └── workflows/
│       └── collect-traffic.yml
├── .gitignore
├── README.md
└── traffic_congestion_crawler.py
```

Có thể dùng VS Code và Git:

```powershell
git init
git add .
git commit -m "Set up traffic collector"
git branch -M main
git remote add origin https://github.com/TEN_GITHUB/hcmc-traffic-congestion-data.git
git push -u origin main
```

Thay `TEN_GITHUB` bằng tên tài khoản GitHub của bạn.

## 3. Thêm TomTom API key

Trong repository GitHub:

1. Mở **Settings**.
2. Chọn **Secrets and variables** → **Actions**.
3. Chọn **New repository secret**.
4. Name: `TOMTOM_API_KEY`.
5. Secret: dán TomTom API key của bạn.
6. Chọn **Add secret**.

Không ghi API key trực tiếp vào Python, README hoặc workflow.

## 4. Cho phép workflow ghi CSV

Thông thường khai báo `contents: write` trong workflow là đủ. Nếu GitHub vẫn báo
lỗi quyền:

1. Mở **Settings** → **Actions** → **General**.
2. Tìm **Workflow permissions**.
3. Chọn **Read and write permissions**.
4. Chọn **Save**.

Repository có branch protection chặn push thì cần cho phép GitHub Actions ghi vào
nhánh mặc định hoặc tạm thời không dùng branch protection trong giai đoạn thu thập.

## 5. Chạy thử

1. Mở tab **Actions**.
2. Chọn workflow **Collect HCMC traffic data**.
3. Chọn **Run workflow** → **Run workflow**.
4. Chờ workflow hiện dấu tích xanh.
5. Kiểm tra `data/traffic_congestion.csv` trong repository.

Một lần chạy sẽ thêm 12 dòng, tương ứng 12 điểm đại diện trên 12 tuyến đường.

## 6. Lịch tự động

Workflow chạy tại các phút `03, 13, 23, 33, 43, 53` của mỗi giờ, tức chu kỳ 10
phút. Việc lệch 3 phút giúp giảm khả năng bị chậm vào đúng đầu giờ.

GitHub Actions dùng UTC cho cron, nhưng vì đây là chu kỳ lặp 10 phút nên không ảnh
hưởng. Python vẫn lưu đồng thời thời gian UTC và thời gian TP.HCM.

Lịch GitHub không bảo đảm chính xác tuyệt đối từng phút; lúc hệ thống đông, một lần
chạy có thể bắt đầu muộn.

## 7. Theo dõi lỗi

- `status = ok`: TomTom trả về dữ liệu hợp lệ; các cột tốc độ và mức ùn tắc có giá
  trị.
- `status = error`: xem `error_type` và `error_message`.
- Workflow vẫn commit các dòng lỗi để không làm mất dấu lần thu thập, sau đó được
  đánh dấu thất bại để bạn dễ phát hiện trong tab Actions.
- Nếu báo `authentication_error`, kiểm tra lại secret `TOMTOM_API_KEY`.

## 8. Dừng thu thập

Vào **Actions** → **Collect HCMC traffic data** → nút ba chấm → **Disable
workflow**.

Nên tải `data/traffic_congestion.csv` về sau 2–4 tuần và giữ một bản sao trước khi
làm sạch hoặc phân tích.

## 9. Chạy thủ công trên Windows khi cần

```powershell
$env:TOMTOM_API_KEY="API_KEY_CUA_BAN"
python .\traffic_congestion_crawler.py --output .\data\traffic_congestion.csv
```

