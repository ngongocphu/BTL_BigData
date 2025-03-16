library(sparklyr)

# Thiết lập đường dẫn Spark
Sys.setenv(SPARK_HOME = "C:/spark-3.5.4-bin-hadoop3")

# Tạo kết nối Spark
sc <- spark_connect(
  master = "local",
  spark_home = Sys.getenv("SPARK_HOME")
)

# Kiểm tra kết nối
print(sc)
spark_web(sc)
####
####

# Bước 1: Tải các thư viện cần thiết
library(sparklyr)
library(ggplot2)
library(dplyr)

# Bước 2: Thiết lập đường dẫn và kết nối Spark
Sys.setenv(SPARK_HOME = "C:/spark-3.5.4-bin-hadoop3")

sc <- spark_connect(
  master = "local",
  config = list(sparklyr.shell.memory = "4G")
)

# Bước 3: Đọc dữ liệu từ tệp CSV vào Spark DataFrame
file_path <- "D:/Ky2_24-25/BigData/TH/cleaned_hotel_booking.csv"

data <- spark_read_csv(sc, name = "hotel_data", path = file_path, 
                       header = TRUE, infer_schema = TRUE)

# Kiểm tra xem dữ liệu có được đọc đúng không
if (is.null(data) || is.logical(data)) {
  stop("Lỗi: Không thể đọc dữ liệu từ file CSV. Kiểm tra đường dẫn và định dạng file.")
}

# Hiển thị thông tin dữ liệu
glimpse(data)


# Bước 4: Xử lý dữ liệu
data <- data %>%
  mutate(
    reservation_status_date = to_date(reservation_status_date),
    month = month(reservation_status_date),
    year = year(reservation_status_date)
  )

# Chuyển đổi biến phân loại sang số bằng String Indexer
data <- data %>%
  ft_string_indexer(input_col = "hotel", output_col = "hotel_index") %>%
  ft_string_indexer(input_col = "reserved_room_type", output_col = "room_type_index")

# Bước 5: Lưu dữ liệu đã xử lý vào tệp Parquet (ghi đè nếu có)
output_path <- "D:/Ky2_24-25/BigData/TH/hotel_data.parquet"

spark_write_parquet(data, output_path, mode = "overwrite")

message("Dữ liệu đã được lưu vào: ", output_path)

# Bước 6: Chuyển đổi dữ liệu từ Spark DataFrame sang DataFrame của R (chỉ lấy cột cần thiết)
data_r <- data %>% select(hotel, is_canceled, adr, month, year, reserved_room_type) %>% collect()


# Bước 7: Vẽ biểu đồ  

## 1️⃣ Số lượng đặt phòng theo tháng  
monthly_bookings <- data_r %>% 
  group_by(year, month) %>% 
  summarise(count = n(), .groups = 'drop') 

ggplot(monthly_bookings, aes(x = month, y = count, color = as.factor(year))) + 
  geom_line() + 
  geom_point() + 
  labs(title = "Số lượng đặt phòng theo tháng", x = "Tháng", y = "Số lượng đặt phòng") + 
  scale_x_continuous(breaks = 1:12) + 
  theme_minimal()

## 2️⃣ Tỷ lệ hủy đặt phòng theo tháng  
monthly_cancellations <- data_r %>% 
  group_by(year, month) %>%
  summarise(
    total_bookings = n(),
    total_cancellations = sum(is_canceled),
    .groups = 'drop'
  ) %>%
  mutate(cancellation_rate = total_cancellations / total_bookings)

ggplot(monthly_cancellations, aes(x = month, y = cancellation_rate, color = as.factor(year))) + 
  geom_line() + 
  geom_point() + 
  labs(title = "Tỷ lệ hủy đặt phòng theo tháng", x = "Tháng", y = "Tỷ lệ hủy") + 
  scale_x_continuous(breaks = 1:12) + 
  theme_minimal()

## 3️⃣ Số lượng đặt phòng theo loại khách sạn  
ggplot(data_r, aes(x = hotel, fill = as.factor(is_canceled))) + 
  geom_bar(position = "dodge") + 
  labs(title = "Số lượng đặt phòng theo loại khách sạn", x = "Loại khách sạn", y = "Số lượng đặt phòng") + 
  scale_fill_manual(name = "Trạng thái đặt phòng", values = c("blue", "red"), labels = c("Không bị hủy", "Bị hủy")) + 
  theme_minimal()

## 4️⃣ Phân phối ADR (Average Daily Rate) theo loại khách sạn  
ggplot(data_r, aes(x = hotel, y = adr)) + 
  geom_boxplot() + 
  labs(title = "ADR (Tỷ lệ trung bình hàng ngày) theo loại khách sạn", x = "Loại khách sạn", y = "Giá trung bình mỗi ngày (ADR)") + 
  theme_minimal()

## 5️⃣ Số lượng đặt phòng theo loại phòng  
ggplot(data_r, aes(x = reserved_room_type, fill = as.factor(is_canceled))) + 
  geom_bar(position = "dodge") + 
  labs(title = "Số lượng đặt phòng theo loại phòng", x = "Loại phòng đã đặt", y = "Số lượng đặt phòng") + 
  scale_fill_manual(name = "Trạng thái đặt phòng", values = c("blue", "red"), labels = c("Không bị hủy", "Bị hủy")) + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) + 
  theme_minimal()

# 📌 Bổ sung: Huấn luyện mô hình Random Forest trên dữ liệu
# Bước 8: Chia dữ liệu thành tập huấn luyện (80%) và tập kiểm tra (20%)
partitions <- data %>%
  sdf_partition(train = 0.8, test = 0.2, seed = 1234)

train_data <- partitions$train
test_data <- partitions$test

# Bước 9: Huấn luyện mô hình Random Forest
rf_model <- train_data %>%
  ml_random_forest_classifier(
    response = "is_canceled",
    features = c("hotel_index", "adr", "month", "year", "room_type_index"),
    num_trees = 100,   # Số cây trong rừng
    max_depth = 10,    # Độ sâu tối đa của cây
    seed = 1234
  )

# Bước 10: Dự đoán trên tập kiểm tra
predictions <- ml_predict(rf_model, test_data)

# Bước 11: Đánh giá mô hình bằng AUC-ROC
auc <- ml_binary_classification_evaluator(predictions, label_col = "is_canceled", metric_name = "areaUnderROC")

message("🎯 Hiệu suất mô hình Random Forest - AUC-ROC: ", round(auc, 4))

##
# Lấy danh sách RDD đang lưu trữ
rdd_info <- spark_web(sc, "storage")
print(rdd_info)
# Xem trước dữ liệu trong Spark
head(data)
# Thu thập dữ liệu từ Spark về R
local_data <- collect(data)
View(local_data)  # Xem bảng dữ liệu

#Hiển thị cột
colnames(data_r)

# Bước 12: Ngắt kết nối Spark
spark_disconnect(sc)
message("Phiên làm việc Spark đã kết thúc.")
