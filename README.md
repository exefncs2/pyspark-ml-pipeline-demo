# PySpark ML Pipeline Demo（S_ML 學習與實作報告）

> 這是一個以展示流程串接方法為主的練習專案，用紅白酒公開資料集（幾千筆）把 **PostgreSQL（ODS / DWH / DM）→ Redis → PySpark → RabbitMQ → FastAPI / Vue 3** 整條路實際接通。

## 1. 系統流程圖（線上更新）

<img src="https://docs.google.com/drawings/d/1RFKfnPxElctGqfn72ukrSXaJ9x4Rbvs2ob_SG469kb4/pub?w=1440&amp;h=1080" alt="S_ML Architecture Diagram">

---

## 2. 這一版程式碼實際做了什麼

### (1) PostgreSQL 三層 Schema 規劃
在 PostgreSQL 先建立三個 Schema 練習分層存放：
- **ODS**：放 `insert_test_data.ipynb` 直接下載匯入的 `wine_quality_red` 與 `wine_quality_white` 原始資料表。
- **DWH**：放 `test002.py` 寫回的預測結果表 `DWH.wine_quality_predictions`。
- **DM**：預留給後續前端報表或特定業務聚合使用（本版尚未實作 DM 表）。

範例資料來源：
- [紅酒資料集（Wine Quality Red）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv)
- [白酒資料集（Wine Quality White）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv)

### (2) 各腳本實際執行的流程
1. **匯入測試資料（`notebooks/insert_test_data.ipynb`）**：下載上述兩份 CSV，透過 `PsqlConnector` 建表並寫入 PostgreSQL `ODS`。
2. **讀取、預測並送進佇列（`S_ML/jobs/test_001.py`）**：
   - 呼叫 `load_table_to_redis()` 把 `ODS.wine_quality_red` 與 `ODS.wine_quality_white` 轉成 JSON 字串存進 Redis。
   - 再從 Redis 讀出 `wine_quality:wine_quality_red`，用 PySpark `DecisionTreeRegressor`（80/20 切分）預測 `residual_sugar`。
   - 將測試集每筆預測結果用 `orjson` 序列化後，逐筆發佈到 RabbitMQ 佇列 `wine_quality_predictions`（Exchange：`wine_quality_exchange`）。
3. **常駐接收並寫入 DWH（`S_ML/worker/test002.py`）**：
   - 透過 `RabbitMQConsumerEngine` 監聽 `wine_quality_predictions`，收到訊息後放進本機 `queue.Queue` 並回傳 ACK。
   - 另一個迴圈每秒檢查一次本機佇列，有資料時開啟 `SparkEngine` 轉成 DataFrame，再透過參數化 SQL 寫入 `DWH.wine_quality_predictions`。
4. **查詢 API 與圖表頁面（`S_ML/worker/fast_api_demo.py` + `S_ML/interface/vue3_demo.html`）**：
   - FastAPI 提供 `/predictions/` 查詢 `DWH.wine_quality_predictions`，根路徑 `/` 回傳單一 HTML 檔（CDN 引入 Vue 3、Bootstrap、Chart.js）畫出折線圖與表格。

依序執行 `insert_test_data.ipynb` → `base_flow.ipynb` → `start_work.ipynb` 後的實際展示畫面如下：

![酒類預測結果展示](image/image.png)

### (3) 專案分層結構（Structure）
- **設定（`S_ML/config.py`）**：集中讀取 `.env` 的 PostgreSQL、Redis、RabbitMQ、Spark 連線設定與佇列常數。
- **基礎（`S_ML/base/`）**：`PsqlConnector`、`RedisEngine`、`RabbitMQConsumerEngine`、`SparkEngine` 四個連線封裝類別。
- **巨集（`S_ML/center/`）**：`psql_to_redis.py`、`redis_ml_to_rabbitmq.py` 兩個固定流程函式檔。
- **工具（`S_ML/model/`、`S_ML/tests/`、`S_ML/util/`）**：Pydantic v2 資料模型、單元與整合測試、共用工具預留目錄。
- **工作類型（`S_ML/jobs/`、`S_ML/worker/`）**：
  - `jobs/test_001.py`：一次性推播腳本。
  - `worker/test002.py`：常駐監聽寫入腳本；`worker/fast_api_demo.py`：查詢 API。
- **介面（`S_ML/interface/`）**：`vue3_demo.html` 前端頁面。

---

## 3. 實作注意事項與目前範圍（Future）

**執行時要注意的幾件事：**
- RabbitMQ 服務需自行啟動（本機或容器）。
- `test002.py` 是常駐監聽腳本，跑起來不會自動結束，按 `Ctrl+C` 停止。
- 本版重點是把幾個元件的串接跑通：目前 `test002.py` 在每次處理本機佇列時都會重新啟停 `SparkSession` 並逐筆 `INSERT`，僅適合示範資料量，非高流量架構；模型也僅套用預設參數的決策樹。

**後續待辦規劃：**
- [x] 加入 Vue 3 簡易資料瀏覽與圖表功能
- [x] 整理連線封裝、集中設定檔、參數化 SQL 與消除棄用警告
- [ ] 引入 Prefect 或 Airflow 排程管理 PySpark，或進一步整合 Kubernetes
- [ ] 補齊整合測試與一鍵啟動全部服務的 Docker Compose 環境

---

## 4. 正式使用方式

### 步驟 1：準備外部服務與 `.env`
確認已啟動 **Apache Spark、RabbitMQ、PostgreSQL、Redis**，並於 PostgreSQL 建立 `ODS`、`DWH`、`DM` 三個 Schema：

```bash
cp .env.example .env
# 編輯 .env 填入實際連線資訊
pip install -r requirements.txt
```

### 步驟 2：依序跑完流程
1. 執行 [`notebooks/insert_test_data.ipynb`](notebooks/insert_test_data.ipynb) 灌入測試資料至 `ODS`。
2. 執行 [`notebooks/base_flow.ipynb`](notebooks/base_flow.ipynb)（或於專案根目錄分別執行 `python S_ML/jobs/test_001.py` 與 `python S_ML/worker/test002.py`）。
3. 執行 [`notebooks/start_work.ipynb`](notebooks/start_work.ipynb)（或於專案根目錄執行 `uvicorn S_ML.worker.fast_api_demo:app --reload`），打開瀏覽器連至 `http://127.0.0.1:8000`。

```bash
# 執行設定單元測試
cd S_ML
python -m unittest tests/test_config.py -v
```
