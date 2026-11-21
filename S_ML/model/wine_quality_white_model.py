"""白酒品質特徵資料模型（Pydantic v2）。"""

from typing import Optional

from pydantic import BaseModel, ConfigDict


class WineQualityWhite(BaseModel):
    """白酒理化指標與品質評分模型。"""

    model_config = ConfigDict(from_attributes=True)

    fixed_acidity: Optional[float] = None
    volatile_acidity: Optional[float] = None
    citric_acid: Optional[float] = None
    residual_sugar: Optional[float] = None
    chlorides: Optional[float] = None
    free_sulfur_dioxide: Optional[float] = None
    total_sulfur_dioxide: Optional[float] = None
    density: Optional[float] = None
    ph: Optional[float] = None
    sulphates: Optional[float] = None
    alcohol: Optional[float] = None
    quality: Optional[int] = None
