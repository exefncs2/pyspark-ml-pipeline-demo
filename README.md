# PySpark ML Pipeline Demo（S_ML 學習與實作報告）

> 這是一個以方法展示為目的的資料工程學習專案，用來練習把 **PostgreSQL（ODS / DWH / DM 分層）、Redis、RabbitMQ、PySpark** 與 **FastAPI / Vue 3** 串成一條完整的資料流。

## 1. 系統流程圖（線上更新）

<img src="https://docs.google.com/drawings/d/1RFKfnPxElctGqfn72ukrSXaJ9x4Rbvs2ob_SG469kb4/pub?w=1440&amp;h=1080" alt="S_ML Architecture Diagram">

---

## 2. 學習設計思路與架構拆解

### (1) 資料倉儲三層 Schema 概念（PostgreSQL）
在 PostgreSQL 上練習建立三個分層 Schema，區隔原始資料與處理後的資料：
- **ODS（Operational Data Store）**：存放剛匯入的操作原始資料，保留細節且方便重跑。
- **DWH（Data Warehouse）**：存放經清洗、整合與 Spark 模型預測後的資料（例如 `DWH.wine_quality_predictions`）。
- **DM（Data Mart）**：針對特定業務主題或前端展示最佳化的資料市集預留層。

### (2) 整條資料流為什麼這樣串？
1. **資料匯入（`notebooks/insert_test_data.ipynb`）**：先從 UCI 下載紅酒／白酒公開資料集灌進 PostgreSQL `ODS`。
   - [紅酒資料集（Wine Quality Red）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv)
   - [白酒資料集（Wine Quality White）](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv)
2. **快取與非同步推播（`S_ML/jobs/test_001.py`）**：將 `ODS` 資料先快取進 **Redis**（避免反覆重壓資料庫），由 **PySpark** 執行決策樹回歸預測 `residual_sugar`，再把預測結果送往 **RabbitMQ** 佇列 `wine_quality_predictions`。
3. **常駐消費與落地（`S_ML/worker/test002.py`）**：常駐監聽 Exchange `wine_quality_exchange`，收到訊息後以微批次交給 Spark 處理並以參數化 SQL 寫入 `DWH.wine_quality_predictions`，完成後回送手動 ACK 確保不漏訊息。
4. **前端展示（`S_ML/worker/fast_api_demo.py` + `S_ML/interface/vue3_demo.html`）**：透過 FastAPI 讀取 DWH 資料，讓 Vue 3 + Chart.js 頁面呈現預測值與實際殘糖值對照。

整體透過 `insert_test_data.ipynb` → `base_flow.ipynb` → `start_work.ipynb` 串接後的實際畫面如下：

![酒類預測結果展示](image/image.png)

### (3) 專案分層結構（Structure）
- **設定（`S_ML/config.py`）**：集中管理 PostgreSQL、Redis、RabbitMQ、Spark 環境設定與常數。
- **基礎（`S_ML/base/`）**：基礎引擎與核心連線封裝（支援 `with` 自動釋放資源與參數化 SQL）。
- **巨集（`S_ML/center/`）**：處理重複性與固定流程（`psql_to_redis.py`、`redis_ml_to_rabbitmq.py`）。
- **工具（`S_ML/model/`、`S_ML/tests/`、`S_ML/util/`）**：Pydantic v2 輸出入模型、測試與共用輔助工具。
- **工作類型（`S_ML/jobs/`、`S_ML/worker/`）**：
  - `jobs`：一次性或定時批次作業（如 `test_001.py`）。
  - `worker`：常駐監聽作業與查詢服務（如 `test002.py`、`fast_api_demo.py`）。
- **介面（`S_ML/interface/`）**：Vue 3 前端展示頁面（`vue3_demo.html`）。

---

## 3. 實作注意事項與後續規劃（Future）

**實作時踩過與需留意的重點：**
- RabbitMQ 的 Exchange／Queue 若尚未建立，需確認容器或服務已啟動且權限正確。
- `test002.py` 為常駐監聽式 Worker，啟動後不會自動停止，會持續等待佇列新訊息（按 `Ctrl+C` 結束）。
- 本專案模型僅作為管線串接範例，重點在於資料流與元件解耦，尚未做進階特徵工程與調參。

**後續學習規劃：**
- [x] 加入 Vue 3 簡易資料瀏覽與圖表功能
- [x] 整理連線封裝、集中設定檔、參數化 SQL 與移除匯入副作用
- [ ] 引入 Prefect 或 Airflow 排程管理 PySpark，或進一步整合 Kubernetes
- [ ] 補齊更完整的整合測試與 Docker Compose 一鍵啟動環境

---

## 4. 正式使用方式

### 步驟 1：準備外部服務與環境變數
請先確認本機或容器已啟動 **Apache Spark、RabbitMQ、PostgreSQL、Redis**，並於 PostgreSQL 建立 `ODS`、`DWH`、`DM` 三個 Schema，接著設定 `.env`：

```bash
cp .env.example .env
# 編輯 .env 填入實際的 PG / REDIS / RABBITMQ / SPARK 連線資訊
```

### 步驟 2：安裝相依套件
```bash
pip install -r requirements.txt
```

### 步驟 3：依序執行資料流與檢視結果
1. 執行 [`notebooks/insert_test_data.ipynb`](notebooks/insert_test_data.ipynb) 匯入紅白酒測試資料至 `ODS`。
2. 執行 [`notebooks/base_flow.ipynb`](notebooks/base_flow.ipynb)（或於專案根目錄分別執行 `python S_ML/jobs/test_001.py` 與 `python S_ML/worker/test002.py`）跑完推播與 DWH 寫入。
3. 執行 [`notebooks/start_work.ipynb`](notebooks/start_work.ipynb)（或於根目錄執行 `uvicorn S_ML.worker.fast_api_demo:app --reload`），打開瀏覽器連到 `http://127.0.0.1:8000` 查看結果。

```bash
# 執行設定單元測試
cd S_ML
python -m unittest tests/test_config.py -v
```
