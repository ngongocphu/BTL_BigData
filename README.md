# BTL_BigData_
# Đề tài :Dự đoán Xu Hướng Đặt Phòng Khách Sạn với Sparklyr

## 1️⃣ Giới thiệu
Dự án này sử dụng ngôn ngữ **R** kết hợp với **Apache Spark** thông qua thư viện `sparklyr` để phân tích và dự đoán xu hướng đặt phòng khách sạn. 

Mục tiêu:
- **Xử lý dữ liệu lớn** bằng Spark.
- **Phân tích xu hướng đặt phòng** qua biểu đồ trực quan hóa với `ggplot2`.
- **Dự đoán khả năng hủy đặt phòng** bằng mô hình **Random Forest**.

## 2️⃣ Công nghệ sử dụng
- **Ngôn ngữ**: R
- **Thư viện**: `sparklyr`, `ggplot2`, `dplyr`
- **Framework xử lý dữ liệu lớn**: Apache Spark
- **Mô hình dự đoán**: Random Forest

## 3️⃣ Cài đặt
### **Yêu cầu hệ thống**
- Cài đặt **R** và **RStudio**.
- Cài đặt **Apache Spark** (Tải từ [Spark Download](https://spark.apache.org/downloads.html)).
- Cài đặt **gói sparklyr** trong R:
  ```r
  install.packages("sparklyr")
  install.packages("ggplot2")
  install.packages("dplyr")
  ```
- Cấu hình đường dẫn Spark:
  ```r
  Sys.setenv(SPARK_HOME = "C:/spark-3.5.4-bin-hadoop3")
  ```

## 4️⃣ Chạy chương trình
### **1. Kết nối với Spark**
```r
library(sparklyr)
sc <- spark_connect(master = "local", spark_home = Sys.getenv("SPARK_HOME"))
```

### **2. Đọc dữ liệu từ CSV**
```r
file_path <- "D:/Ky2_24-25/BigData/TH/cleaned_hotel_booking.csv"
data <- spark_read_csv(sc, name = "hotel_data", path = file_path, header = TRUE, infer_schema = TRUE)
```

### **3. Xử lý dữ liệu**
- Chuyển đổi cột ngày tháng
- Biến đổi biến phân loại thành số
- Lưu dữ liệu dưới dạng **Parquet** để tăng hiệu suất xử lý
```r
data <- data %>%
  mutate(
    reservation_status_date = to_date(reservation_status_date),
    month = month(reservation_status_date),
    year = year(reservation_status_date)
  )
spark_write_parquet(data, "D:/Ky2_24-25/BigData/TH/hotel_data.parquet", mode = "overwrite")
```

### **4. Trực quan hóa dữ liệu**
- **Số lượng đặt phòng theo tháng**
- **Tỷ lệ hủy đặt phòng theo tháng**
- **Số lượng đặt phòng theo loại khách sạn**
```r
monthly_bookings <- data_r %>% group_by(year, month) %>% summarise(count = n(), .groups = 'drop')
ggplot(monthly_bookings, aes(x = month, y = count, color = as.factor(year))) + 
  geom_line() + geom_point() + theme_minimal()
```

### **5. Huấn luyện mô hình Random Forest**
```r
rf_model <- train_data %>%
  ml_random_forest_classifier(
    response = "is_canceled",
    features = c("hotel_index", "adr", "month", "year", "room_type_index"),
    num_trees = 100,
    max_depth = 10,
    seed = 1234
  )
```

### **6. Dự đoán và đánh giá mô hình**
```r
predictions <- ml_predict(rf_model, test_data)
auc <- ml_binary_classification_evaluator(predictions, label_col = "is_canceled", metric_name = "areaUnderROC")
message("🎯 Hiệu suất mô hình Random Forest - AUC-ROC: ", round(auc, 4))
```

## 5️⃣ Kết thúc phiên làm việc
```r
spark_disconnect(sc)
message("Phiên làm việc Spark đã kết thúc.")
```
## Kết quả ##
# Phân tích dữ liệu đặt phòng khách sạn  

## 1️⃣ Số lượng đặt phòng theo tháng  
![Số lượng đặt phòng theo tháng](images/1.png)  

## 2️⃣ Tỷ lệ hủy đặt phòng theo tháng  
![Tỷ lệ hủy đặt phòng theo tháng](images/2.png)  

## 3️⃣ Số lượng đặt phòng theo loại khách sạn  
![Số lượng đặt phòng theo loại khách sạn](images/3.png)  

## 4️⃣ Phân phối ADR (Giá trung bình mỗi ngày) theo loại khách sạn  
![Phân phối ADR theo loại khách sạn](images/4.png)  

## 5️⃣ Số lượng đặt phòng theo loại phòng  
![Số lượng đặt phòng theo loại phòng](images/5.png)  

## 6️⃣ Hiệu suất mô hình  
![Hiệu suất mô hình](images/6.png)  

## 7️⃣ Xem dữ liệu từ Spark  
![Xem dữ liệu từ Spark](images/7.png)  

## 8️⃣ Xem bảng dữ liệu  
![Xem bảng dữ liệu](images/8.png)  

## 6️⃣ Kết luận
Dự án này giúp **phân tích xu hướng đặt phòng khách sạn** và **dự đoán tỷ lệ hủy đặt phòng** bằng cách tận dụng sức mạnh xử lý dữ liệu lớn của **Apache Spark** và **Sparklyr**. Trong tương lai, có thể thử nghiệm với các mô hình học sâu để cải thiện độ chính xác.

---
📌 **Tác giả: Nguyễn Thị Lan Anh, Ngô Ngọc Phú, Đỗ Ngọc Nghĩa.**
