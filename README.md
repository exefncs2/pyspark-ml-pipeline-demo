# PySpark ML Pipeline Demo (S_ML)

結合 **PySpark、RabbitMQ、Redis、PostgreSQL（ODS / DWH / DM 三層資料倉儲）** 與 **Vue 3** 前端的端對端資料流與機器學習預測展示專案。

## 系統架構流程圖

<img src="https://docs.google.com/drawings/d/1RFKfnPxElctGqfn72ukrSXaJ9x4Rbvs2ob_SG469kb4/pub?w=1440&amp;h=1080" alt="Architecture Diagram">

## 資料倉儲分層設計（PostgreSQL）

於 PostgreSQL 建立三層 Schema：

- **ODS（Operational Data Store）**：存放原始操作資料，支援即時資料落地。
- **DWH（Data Warehouse）**：存放清洗與整合後的歷史與預測資料（如 `DWH.wine_quality_predictions`）。
- **DM（Data Mart）**：針對前端展示與特定業務主題最佳化後的資料市集。

## 專案結構

```text
S_ML/
├── config.py    # 參數設定檔：集中管理 PostgreSQL / Redis / RabbitMQ / Spark 不可變設定
├── base/        # 基礎引擎與核心元件（Spark / DB / MQ / Redis 連線封裝）
├── center/      # 共用巨集與標準化處理流程
├── model/       # 資料輸入輸出與機器學習模型（Pydantic）
├── jobs/        # 一次性或排程批次作業（例：test_001.py 寫入 Redis 並推送 RabbitMQ）
├── worker/      # 常駐監聽作業與查詢 API（test002.py 消費寫入 DWH、fast_api_demo.py）
├── interface/   # Vue 3 前端介面
├── util/        # 共用工具函式
└── tests/       # 單元與整合測試
notebooks/       # 資料匯入與端對端流程驗證 Notebook
```

## 快速開始

### 1. 環境準備

請確認本機或容器環境已備妥 **Apache Spark、RabbitMQ、PostgreSQL、Redis**，並建立環境設定檔：

```bash
cp .env.example .env
# 編輯 .env 填入實際連線資訊
```

安裝 Python 與前端相依套件：

```bash
pip install -r requirements.txt
npm install
```

### 2. 匯入測試資料

執行 [`notebooks/insert_test_data.ipynb`](notebooks/insert_test_data.ipynb) 載入 UCI Wine Quality 資料集：

- [紅酒品質資料集（Wine Quality Red）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv)
- [白酒品質資料集（Wine Quality White）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv)

### 3. 啟動資料流與前端展示

1. **批次推播 Job（`test_001.py`）**：將資料寫入 Redis 並送至 RabbitMQ 佇列 `wine_quality_predictions`。
2. **常駐監聽 Worker（`test002.py`）**：監聽 `wine_quality_exchange` 並將結果寫入 `DWH.wine_quality_predictions`（常駐等待新訊息）。
3. 透過 [`notebooks/base_flow.ipynb`](notebooks/base_flow.ipynb) 驗證整體資料流，再以 [`notebooks/start_work.ipynb`](notebooks/start_work.ipynb) 啟動前端服務並開啟 `http://127.0.0.1:8000` 檢視結果：

![酒類預測結果展示](image/image.png)

## 規劃與進度

- [x] 建立 ODS / DWH / DM 分層與 PySpark + RabbitMQ + Redis 資料流
- [x] 加入 Vue 3 簡易資料瀏覽介面
- [ ] 引入 Airflow / Prefect 排程管理與 Kubernetes 部署
- [ ] 補齊 Docker Compose 一鍵啟動環境與完整 pytest 測試覆蓋
