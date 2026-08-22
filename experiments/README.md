# Direct Pipeline Loss Experiment

Thí nghiệm độc lập nhằm chứng minh giới hạn và hiện tượng mất mát dữ liệu của kiến trúc kết nối trực tiếp:

```text
SCADA Source → Python In-Memory Queue → InfluxDB
```

---

## Hiểu đúng kiến trúc của thí nghiệm

### 1. Kiến trúc ban đầu

Trong kiến trúc ban đầu của hệ thống, toàn bộ quá trình từ sinh dữ liệu đến lưu trữ được thực thi theo mô hình xử lý tuần tự (synchronous/blocking pipeline):

```text
SCADA Simulator
      ↓
XGBoost
      ↓
Anomaly Detection
      ↓
InfluxDB
```

Nó xử lý tuần tự:

```text
Tạo Event 1
→ xử lý Event 1
→ ghi InfluxDB
→ tạo Event 2
→ xử lý Event 2
→ ghi InfluxDB
```

**Đặc điểm và nguyên nhân InfluxDB thường nhận gần 100% dữ liệu ở các lần test trước:**
- Simulator phải chờ event hiện tại xử lý và ghi xong.
- Nếu XGBoost và InfluxDB chỉ xử lý được khoảng 20 event/giây thì simulator cũng bị kéo xuống gần tốc độ đó.
- Đặt `events_per_second=200` không bảo đảm nguồn thực sự tạo đủ 200 event mỗi giây.
- Đây là lý do các lần test trước InfluxDB thường nhận gần 100%: nguồn phát bị tốc độ xử lý phía sau kìm lại.
- Pipeline chậm lại thay vì tạo backlog rõ ràng.

---

### 2. Kiến trúc thí nghiệm mới

Để mô phỏng đúng thực tế khi nguồn SCADA phát độc lập ngoài biên mà không phụ thuộc backend xử lý kịp hay không, thí nghiệm benchmark tách rời nguồn phát và khối xử lý bằng hai luồng độc lập:

```text
┌─────────────────────────────────────────────┐
│             TIẾN TRÌNH PYTHON               │
│                                             │
│  Producer Thread                            │
│  Tạo dữ liệu theo tốc độ nguồn              │
│          │                                  │
│          ▼                                  │
│  ┌──────────────────────┐                   │
│  │ Queue trong RAM      │                   │
│  │ Capacity hữu hạn     │                   │
│  └──────────┬───────────┘                   │
│             ▼                               │
│  Consumer Thread                            │
│  XGBoost → Anomaly → InfluxDB               │
│                                             │
└─────────────────────────────────────────────┘
```

**Nguyên lý vận hành:**
- **Cùng một tiến trình**: Producer, queue và consumer đều nằm trong cùng một Python process.
- **Hai thread độc lập**:
  - **Producer Thread**: Tạo event theo tốc độ nguồn.
  - **Consumer Thread**: Lấy event ra để dự đoán và ghi InfluxDB.
- **Không bị chặn**: Producer không phải chờ từng lần inference/write hoàn thành.
- **Lưu trữ tạm**: Queue nằm trong RAM, không được lưu bền vững trên ổ đĩa.
- **Rủi ro sập tiến trình**: Nếu Python process crash, event còn trong queue sẽ biến mất.

---

### 3. Queue RAM hoạt động thế nào?

Hình dung Queue như một "cái hộp" có sức chứa giới hạn:

```text
Queue(maxsize=100)

┌──────────────────┐
│ Event 101        │
│ Event 102        │
│ Event 103        │
│ ...              │
│ Tối đa 100 event │
└──────────────────┘
```

Producer bỏ event vào queue, consumer lấy event ra.

**Ví dụ bài toán tốc độ:**
```text
Producer: 200 event/giây
Consumer:  20 event/giây
```

Backlog tăng gần:
```text
200 - 20 = 180 event/giây
```

Trong khi queue chỉ chứa tối đa 100 event nên nó sẽ đầy nhanh.

```text
Producer
   │
   ├── Event 1   → Queue → Consumer
   ├── Event 2   → Queue
   ├── Event 3   → Queue
   │
   ├── Event 100 → Queue FULL
   ├── Event 101 → DROP
   ├── Event 102 → DROP
   └── Event 103 → DROP
```

**Cơ chế `put_nowait()`:**
- Queue còn chỗ $\to$ `queue_accepted` tăng.
- Queue đầy $\to$ `queue_dropped` tăng.
- Producer không đứng chờ consumer.

---

### 4. Các chỉ số cần đọc

Ý nghĩa và vai trò của từng chỉ số trong thí nghiệm:

| Chỉ số | Ý nghĩa |
|---|---|
| `source_generated` | Tổng số event Producer đã tạo ra từ nguồn. |
| `queue_accepted` | Số event được queue RAM tiếp nhận thành công khi còn chỗ. |
| `queue_dropped` | Số event bị drop ngay khi queue đã đầy. |
| `events_processed` | Số event Consumer đã lấy ra khỏi queue và xử lý xong. |
| `influx_write_success` | Số event ghi thành công vào InfluxDB. |
| `influx_write_failed` | Số event ghi thất bại vào InfluxDB (do lỗi mạng/outage). |
| `queue_pending` | Số event còn tồn đọng trong queue khi kết thúc đo lường. |

**Quan hệ bảo toàn:**
```text
source_generated = queue_accepted + queue_dropped
```

**Ví dụ:**
```text
source_generated = 5000
queue_accepted   = 700
queue_dropped    = 4300
```

> [!IMPORTANT]
> **Kết luận phù hợp:**
> *Nguồn đã tạo 5.000 event, nhưng 4.300 event không thể vào pipeline vì queue RAM hữu hạn đã đầy.*
> 
> **Lưu ý:** Không được nói InfluxDB làm mất toàn bộ 4.300 event, vì chúng đã bị drop trước khi đến consumer/InfluxDB.

---

### 5. Lưu ý quan trọng về tính chính xác

> [!NOTE]
> Queue RAM không phải thành phần vốn có trong pipeline ban đầu. Nó được chủ động thêm vào benchmark để mô hình hóa một nguồn SCADA hoạt động độc lập và một bộ đệm hữu hạn giữa nguồn với consumer.

- **Không viết**: *"Kiến trúc cũ vốn có queue RAM và queue bị đầy."*
- **Nên viết**: *"Một bounded in-memory queue được sử dụng trong benchmark để mô hình hóa bộ đệm hữu hạn của pipeline trực tiếp khi nguồn dữ liệu hoạt động độc lập với consumer."*

Mục tiêu không phải cố tình làm giả lỗi, mà là tạo một điều kiện có thể đo được:

```text
Nguồn phát nhanh hơn consumer
→ backlog tăng
→ bộ đệm hữu hạn đầy
→ event bị drop
```

---

### 6. Queue RAM và Kafka

| Queue RAM trong thí nghiệm | Kafka |
|---|---|
| Nằm trong Python process | Chạy thành hệ thống broker riêng |
| Dữ liệu chủ yếu nằm trong RAM | Message được lưu vào durable log |
| Process crash thì queue mất | Consumer crash thì message đã ghi Kafka vẫn còn |
| Capacity nhỏ và hữu hạn | Có thể giữ backlog lớn hơn |
| Queue đầy thì event bị drop | Consumer chậm thì consumer lag tăng |
| Không replay sau crash | Có thể đọc lại từ offset |
| Không có replication | Có thể cấu hình replication |

```text
Baseline thí nghiệm:

Producer
   ↓
Queue RAM
   ↓
Consumer
   ↓
InfluxDB
```

```text
Giai đoạn sau:

Producer
   ↓
Kafka
   ↓
Consumer/Spark
   ↓
InfluxDB
```

**Hành vi khi quá tải:**
```text
Queue RAM đầy
→ event mới bị drop
```

Trong khi Kafka:
```text
Consumer chậm
→ consumer lag tăng
→ message vẫn được giữ
→ consumer phục hồi
→ lag giảm về 0
```

> [!NOTE]
> Không khẳng định Kafka không bao giờ mất dữ liệu. Điều đó còn phụ thuộc producer acknowledgement, replication, retention và cấu hình cluster.

---

### 7. Mục tiêu thật của thí nghiệm

> [!IMPORTANT]
> Thí nghiệm `direct-pipeline-loss` không nhằm chứng minh Kafka luôn nhanh hơn. Nó tạo một baseline có thể đo được về những gì xảy ra khi nguồn phát nhanh hơn hệ thống xử lý và bộ đệm hiện tại không đủ khả năng lưu backlog.

**Các giá trị baseline cần đo:**
- Tốc độ nguồn.
- Throughput thực tế.
- Số event generated.
- Số event accepted.
- Số event dropped.
- Số event ghi InfluxDB thành công/thất bại.
- Drop rate.
- Queue pending.
- Thời gian hoàn thành.

---

### 8. Tóm tắt để thảo luận nhóm

```text
Kiến trúc ban đầu không tạo backlog rõ ràng vì simulator phải chờ pipeline xử lý. Benchmark thêm producer độc lập và queue RAM hữu hạn để mô phỏng nguồn SCADA vẫn tiếp tục phát. Khi producer nhanh hơn consumer, queue đầy và event bị drop. Kết quả này sẽ được dùng làm baseline để sau này thay queue RAM bằng Kafka và so sánh khả năng giữ backlog, replay và phục hồi.
```

---

## 2. Cấu trúc & Chức năng từng module

| Module / Tệp tin | Chức năng chính |
|---|---|
| [`experiments/direct_loss_demo.py`](file:///d:/SCDA_Edge/experiments/direct_loss_demo.py) | **Điều phối thí nghiệm**: Khởi tạo hai luồng độc lập (Producer và Consumer) kết nối qua Bounded Queue. Quản lý bộ đếm `ExperimentCounters` an toàn đa luồng, xử lý `Ctrl+C`, in bảng tổng kết và lưu kết quả JSON. |
| [`experiments/experiment_writer.py`](file:///d:/SCDA_Edge/experiments/experiment_writer.py) | **Writer chuyên biệt**: Ghi kết quả đo lường và suy luận vào measurement `direct_pipeline_experiment` thuộc bucket thí nghiệm (`EXPERIMENT_INFLUX_BUCKET`). Không làm ảnh hưởng đến bucket production. |
| [`config/loss_experiment.json`](file:///d:/SCDA_Edge/config/loss_experiment.json) | **Cấu hình thí nghiệm**: Khai báo các tham số như tốc độ phát, tổng số event, dung lượng queue, độ trễ consumer, và kịch bản mô phỏng. |
| [`tests/test_direct_loss_demo.py`](file:///d:/SCDA_Edge/tests/test_direct_loss_demo.py) | **Kiểm thử điều phối & mất mát**: Kiểm tra các trường hợp tải thấp không mất dữ liệu, tải cao drop dữ liệu, bảo toàn số lượng counter, xử lý lỗi ghi InfluxDB và ngắt an toàn mà không cần InfluxDB thật. |
| [`tests/test_experiment_writer.py`](file:///d:/SCDA_Edge/tests/test_experiment_writer.py) | **Kiểm thử Writer schema**: Kiểm tra cấu trúc Point, phân biệt đúng tags (`experiment_id`, `turbine_id`) và fields (`event_sequence`, các chỉ số float/int), đóng kết nối và xử lý biến môi trường. |

---

## 3. Chuẩn bị môi trường

### Bước 1: Tạo Bucket thí nghiệm trong InfluxDB

Khởi chạy InfluxDB (nếu chưa chạy):
```powershell
docker compose up -d
```

Truy cập giao diện InfluxDB UI tại `http://localhost:8086`, vào mục **Load Data** $\to$ **Buckets** $\to$ **Create Bucket** và đặt tên:
```text
scada_experiment
```

### Bước 2: Cấu hình biến môi trường `.env`

Mở file `.env` tại thư mục gốc `D:\SCDA_Edge` và bổ sung biến `EXPERIMENT_INFLUX_BUCKET`:

```dotenv
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=replace_with_your_actual_token
INFLUX_ORG=scada_org
INFLUX_BUCKET=turbine_metrics
EXPERIMENT_INFLUX_BUCKET=scada_experiment
```

> [!CAUTION]
> - `EXPERIMENT_INFLUX_BUCKET` là bắt buộc khi chạy thí nghiệm. Nếu thiếu biến này, `ExperimentWriter` sẽ báo lỗi ngay lập tức và tuyệt đối không tự ý ghi vào bucket production (`turbine_metrics`).
> - Không commit file `.env` lên hệ thống quản lý mã nguồn (Git).

---

## 4. Hướng dẫn chạy kiểm thử (Unit Tests)

Bộ kiểm thử được thiết kế độc lập, sử dụng fake predictor và mock writer, không phụ thuộc vào InfluxDB hay container đang chạy.

### Chạy toàn bộ test dự án:
```powershell
cd D:\SCDA_Edge
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

### Chạy riêng các bài test thí nghiệm:
```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_direct_loss_demo.py tests\test_experiment_writer.py -v -p no:cacheprovider
```

---

## 5. Các kịch bản thử nghiệm chi tiết

Cập nhật file [`config/loss_experiment.json`](file:///d:/SCDA_Edge/config/loss_experiment.json) tương ứng với từng kịch bản bên dưới trước khi chạy:

```powershell
cd D:\SCDA_Edge
.\.venv\Scripts\python.exe -m experiments.direct_loss_demo
```

### Kịch bản A — Kiểm chứng tải thấp (Control Scenario)

Mục đích: Xác nhận ở tải thấp và hàng đợi đủ rộng, toàn bộ dữ liệu sinh ra được tiếp nhận và xử lý đầy đủ mà không mất mát.

*Cấu hình `config/loss_experiment.json`:*
```json
{
  "experiment_id": "direct-control-001",
  "turbine_count": 5,
  "source_events_per_second": 5,
  "total_events": 100,
  "queue_capacity": 100,
  "consumer_delay_seconds": 0.01,
  "continue_on_write_error": true,
  "drain_queue_after_source_stops": true,
  "scenario": "normal",
  "random_seed": 42,
  "result_directory": "experiment_results"
}
```

*Kỳ vọng kết quả:*
- `Queue dropped` = 0
- `Drop rate` = 0.00%
- `Influx success` = `Generated` = 100

---

### Kịch bản B — Quá tải hàng đợi RAM (Overload Scenario)

Mục đích: Chứng minh hiện tượng mất mát dữ liệu khi Producer phát với tốc độ vượt xa năng lực của Consumer và dung lượng hàng đợi trong RAM có hạn.

*Cấu hình `config/loss_experiment.json`:*
```json
{
  "experiment_id": "direct-overload-001",
  "turbine_count": 5,
  "source_events_per_second": 200,
  "total_events": 5000,
  "queue_capacity": 100,
  "consumer_delay_seconds": 0.05,
  "continue_on_write_error": true,
  "drain_queue_after_source_stops": true,
  "scenario": "normal",
  "random_seed": 42,
  "result_directory": "experiment_results"
}
```

*Giải thích kỹ thuật:*
- Nguồn phát sinh ~200 sự kiện/giây.
- Consumer có `consumer_delay_seconds = 0.05s`, nghĩa là tốc độ xử lý tối đa lý thuyết chỉ khoảng $\frac{1}{0.05} = 20\text{ sự kiện/giây}$ (chưa tính thời gian inference và I/O HTTP).
- Khi queue (dung lượng 100) đầy, lời gọi `queue.put_nowait()` từ chối nhận thêm, khiến Producer lập tức đếm vào `queue_dropped`.

*Kỳ vọng kết quả:*
- `Queue dropped` > 0 (chiếm phần lớn tổng số event).
- `Influx success` < `Generated`.
- `Drop rate` > 80%.

---

### Kịch bản C — Gián đoạn dịch vụ lưu trữ (InfluxDB Outage)

Mục đích: Chứng minh khi InfluxDB bị dừng đột ngột, Consumer gặp lỗi ghi và các sự kiện ghi lỗi không có cơ chế replay trong kiến trúc trực tiếp.

*Cấu hình `config/loss_experiment.json`:*
```json
{
  "experiment_id": "direct-influx-outage-001",
  "turbine_count": 5,
  "source_events_per_second": 50,
  "total_events": 1500,
  "queue_capacity": 200,
  "consumer_delay_seconds": 0,
  "continue_on_write_error": true,
  "drain_queue_after_source_stops": true,
  "scenario": "normal",
  "random_seed": 42,
  "result_directory": "experiment_results"
}
```

*Các bước thực hiện:*
1. Khởi chạy thí nghiệm:
   ```powershell
   .\.venv\Scripts\python.exe -m experiments.direct_loss_demo
   ```
2. Trong khi chương trình đang chạy, mở một terminal khác và dừng InfluxDB:
   ```powershell
   docker compose stop influxdb
   ```
3. Sau khoảng 3-5 giây, khởi động lại InfluxDB:
   ```powershell
   docker compose start influxdb
   ```

> [!WARNING]
> Tuyệt đối **không dùng `docker compose down -v`** để tránh xóa mất dữ liệu trên phân vùng volume InfluxDB.

*Kỳ vọng kết quả:*
- `Influx failed` > 0 (các event trong thời gian InfluxDB ngưng hoạt động bị ghi nhận thất bại).
- Các sự kiện ghi lỗi không tự động được phát lại.

---

## 6. Cách đọc & Đối chiếu số liệu tổng kết

Sau mỗi lần thực thi, chương trình in bảng tổng kết:

```text
===== DIRECT PIPELINE LOSS EXPERIMENT =====
Experiment ID:      direct-overload-001
Source rate:        200.0 events/s
Queue capacity:     100
Generated:          5000
Queue accepted:     700
Queue dropped:      4300
Processed:          700
Influx success:     700
Influx failed:      0
Queue pending:      0
Drop rate:          86.00%
Actual throughput:  19.89 events/s
===========================================
```

### Ý nghĩa các chỉ số:
- **`Generated`**: Tổng số sự kiện Producer đã sinh ra từ nguồn.
- **`Queue accepted`**: Số sự kiện được hàng đợi trong RAM tiếp nhận thành công.
- **`Queue dropped`**: Số sự kiện bị loại bỏ do hàng đợi đã đầy tại thời điểm đẩy vào.
- **`Processed`**: Số sự kiện Consumer đã lấy khỏi queue và hoàn thành suy luận mô hình.
- **`Influx success`**: Số sự kiện ghi thành công vào InfluxDB.
- **`Influx failed`**: Số sự kiện gặp lỗi khi gửi HTTP tới InfluxDB.
- **`Queue pending`**: Số sự kiện còn tồn đọng trong queue khi kết thúc (luôn bằng 0 nếu đã drain hết).

### Công thức kiểm chứng & bảo toàn:
$$\text{Generated} = \text{Queue accepted} + \text{Queue dropped}$$
$$\text{Queue accepted} = \text{Influx success} + \text{Influx failed} + \text{Queue pending}$$
$$\text{Drop rate (\%)} = \frac{\text{Queue dropped}}{\text{Generated}} \times 100$$
$$\text{Actual throughput} = \frac{\text{Influx success}}{\text{Tổng thời gian chạy (giây)}}$$

### File kết quả JSON:
Kết quả chi tiết được tự động lưu vào thư mục `experiment_results/<experiment_id>.json` (không chứa credentials hay token).

---

## 7. Giới hạn của thí nghiệm

1. **Baseline đo lường**: Thí nghiệm này là mốc tham chiếu ban đầu cho pipeline xử lý trực tiếp đơn máy, **hoàn toàn chưa có Apache Kafka**.
2. **Bộ nhớ RAM tạm thời**: Queue được quản lý bằng `queue.Queue` thuần túy trong RAM. Nếu tiến trình Python bị terminate (crash/kill), toàn bộ dữ liệu trong queue sẽ biến mất ngay lập tức.
3. **Không hỗ trợ Replay**: Dữ liệu một khi đã drop hoặc write fail sẽ không thể tự phục hồi.
4. **Phụ thuộc môi trường phần cứng**: Thông lượng thực tế (`actual_throughput`) phụ thuộc vào CPU máy chạy, tốc độ phản hồi của InfluxDB và cấu hình trễ mạng cục bộ. Không dùng số liệu tuyệt đối của một máy đơn lẻ để quy kết cho mọi hệ thống.
