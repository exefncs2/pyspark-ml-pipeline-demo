"""紅酒品質特徵資料模型（Pydantic v2）。"""

from pydantic import BaseModel, ConfigDict


class WineQualityRed(BaseModel):
    """紅酒理化指標與品質評分模型。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float
    total_sulfur_dioxide: float
    density: float
    ph: float
    sulphates: float
    alcohol: float
    quality: int
