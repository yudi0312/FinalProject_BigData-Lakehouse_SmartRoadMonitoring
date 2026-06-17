from pathlib import Path
from threading import Lock

from pydantic import BaseModel
from ultralytics import YOLO


CLASS_NAMES = {
    0: "D00_Longitudinal_Crack",
    1: "D10_Transverse_Crack",
    2: "D20_Alligator_Crack",
    3: "D40_Pothole",
    4: "D50_Other_Damage",
}

SEVERITY_MAP = {
    "D00_Longitudinal_Crack": 40,
    "D10_Transverse_Crack": 50,
    "D20_Alligator_Crack": 75,
    "D40_Pothole": 100,
    "D50_Other_Damage": 30,
}

DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[2] / "models" / "best.pt"


class YoloInferenceError(RuntimeError):
    """Raised when the YOLO model cannot be loaded or used for inference."""


class DamagePrediction(BaseModel):
    damage_type: str
    confidence: float
    severity_score: int


class YoloDamageDetector:
    def __init__(self, model_path: str | Path = DEFAULT_MODEL_PATH):
        self.model_path = Path(model_path)
        self.model: YOLO | None = None
        self._lock = Lock()

    def load_model(self) -> None:
        if self.model is not None:
            return

        with self._lock:
            if self.model is not None:
                return
            if not self.model_path.exists():
                raise YoloInferenceError(f"YOLO model file not found: {self.model_path}")
            self.model = YOLO(str(self.model_path))

    def predict_damage(self, image_path: str | Path) -> DamagePrediction:
        self.load_model()
        if self.model is None:
            raise YoloInferenceError("YOLO model is not loaded.")

        try:
            results = self.model(str(image_path), verbose=False)
        except Exception as exc:
            raise YoloInferenceError(f"YOLO inference failed: {exc}") from exc

        best_prediction: DamagePrediction | None = None
        for result in results:
            boxes = getattr(result, "boxes", None)
            if boxes is None or len(boxes) == 0:
                continue

            for box in boxes:
                class_id = int(box.cls[0].item())
                confidence = round(float(box.conf[0].item()), 2)
                damage_type = CLASS_NAMES.get(class_id, "Unknown")
                prediction = DamagePrediction(
                    damage_type=damage_type,
                    confidence=confidence,
                    severity_score=SEVERITY_MAP.get(damage_type, 0),
                )
                if best_prediction is None or prediction.confidence > best_prediction.confidence:
                    best_prediction = prediction

        return best_prediction or DamagePrediction(
            damage_type="Unknown",
            confidence=0,
            severity_score=0,
        )


detector = YoloDamageDetector()


def load_model() -> None:
    detector.load_model()


def predict_damage(image_path: str | Path) -> DamagePrediction:
    return detector.predict_damage(image_path)
