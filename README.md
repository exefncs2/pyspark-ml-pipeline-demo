# PySpark ML Pipeline Demo (S_ML)

結合 **PostgreSQL（ODS / DWH / DM 分層）、Redis、RabbitMQ、PySpark** 與 **FastAPI / Vue 3** 的端對端資料管線與預測結果展示專案。

## 1. 緣起與想驗證的事情

一般機器學習練習多半停留在單一 Notebook 讀 CSV、跑模型就結束了。這個專案是為了練習把模型推論放進接近實務的資料工程架構裡：從資料庫讀原始資料、經快取與訊息佇列解耦、由 Spark 執行計算、寫回資料倉儲，最後透過 API 與前端圖表把結果呈現出來。

## 系統架構流程圖

<img src="https://docs.google.com/drawings/d/1RFKfnPxElctGqfn72ukrSXaJ9x4Rbvs2ob_SG469kb4/pub?w=1440&amp;h=1080" alt="Architecture Diagram">

---

## 2. 設計思路與關鍵取捨

### (1) 為什麼 PostgreSQL 要切出 ODS / DWH / DM 三層 Schema？
- **ODS（Operational Data Store）**：放剛匯入的原始紅白酒資料表，保留原始欄位狀態。
- **DWH（Data Warehouse）**：放經 Spark 處理後的預測結果（`DWH.wine_quality_predictions`），避免直接污染原始表。
- **DM（Data Mart）**：預留給前端查詢或特定報表主題做聚合最佳化使用。

### (2) 為什麼中間要多經過 Redis 與 RabbitMQ，不直接 DB → Spark → DB？
- **加入 Redis**：把常用特徵表序列化後先快取一份，讓反覆測試或多次 Spark 任務不用一直回頭重掃 PostgreSQL。
- **加入 RabbitMQ**：把「訓練並推播預測結果（`jobs/test_001.py`）」與「接收結果並寫入 DWH（`worker/test002.py`）」拆成生產端與消費端。消費端採多執行緒處理訊息並搭配手動 ACK，就算瞬間湧入多筆預測訊息，也不會卡住上游或因連線中斷掉資料。

### (3) 為什麼把引擎封裝在 `S_ML/base/` 並集中 `config.py`？
- 早期寫法是每個腳本各自 `os.getenv()`、各自建連線，容易忘記關閉連線或把帳密寫散。整理後把 PostgreSQL、Redis、RabbitMQ、Spark 連線集中到 `S_ML/config.py` 與 `base/`，統一支援 `with` 自動釋放資源，寫入 SQL 也全部改為參數化查詢。
- 為了相容原本的 Notebook 啟動方式（從根目錄 `uvicorn S_ML.worker...` 或直接執行 `python S_ML/jobs/test_001.py`），模組匯入設計成同時相容套件與單檔腳本兩種路徑。

---

## 3. 目前已知限制

- **模型僅作為管線範例**：目前直接用 PySpark `DecisionTreeRegressor` 預測 `residual_sugar`，重點在把整條資料管線跑通，尚未做特徵工程或超參數調校。
- **基礎服務需自行準備**：專案尚未打包成一鍵啟動的 `docker-compose.yml`，執行前需先在本機或容器啟動 PostgreSQL、Redis、RabbitMQ 與 Spark 環境。

---

## 4. 專案結構

```text
S_ML/
├── config.py    # 集中管理 PostgreSQL / Redis / RabbitMQ / Spark 環境設定
├── base/        # 底層連線封裝（SparkEngine / PsqlConnector / RedisEngine / RabbitMQConsumerEngine）
├── center/      # 共用流程（PostgreSQL -> Redis、Redis -> PySpark -> RabbitMQ）
├── model/       # Pydantic v2 資料結構定義
├── jobs/        # 批次推播腳本（test_001.py）
├── worker/      # 常駐消費寫入（test002.py）與 FastAPI 查詢服務（fast_api_demo.py）
├── interface/   # Vue 3 + Chart.js 視覺化頁面
└── tests/       # 單元與整合測試
notebooks/       # 資料匯入與端對端流程啟動 Notebook
```

## 5. 執行方式

### (1) 環境準備
```bash
cp .env.example .env
# 編輯 .env 填入本機或容器的連線資訊
pip install -r requirements.txt
```

### (2) 匯入範例資料集
執行 [`notebooks/insert_test_data.ipynb`](notebooks/insert_test_data.ipynb) 載入 UCI Wine Quality 紅白酒資料至 PostgreSQL `ODS`。

### (3) 啟動資料流與前端畫面
1. 執行 [`notebooks/base_flow.ipynb`](notebooks/base_flow.ipynb) 啟動批次推播（`test_001.py`）與常駐消費寫入（`test002.py`）。
2. 執行 [`notebooks/start_work.ipynb`](notebooks/start_work.ipynb) 啟動 FastAPI（`uvicorn S_ML.worker.fast_api_demo:app --reload`），開啟 `http://127.0.0.1:8000` 查看圖表結果：

![酒類預測結果展示](image/image.png)
