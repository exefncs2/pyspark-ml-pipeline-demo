"""酒類品質預測輸出資料模型（Pydantic v2）。"""

from pydantic import BaseModel, ConfigDict


class WineQualityPrediction(BaseModel):
    """單筆預測結果與實際殘糖值對照模型。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    prediction: float
    residual_sugar: float
