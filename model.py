import joblib
from huggingface_hub import hf_hub_download

path = hf_hub_download("TarekMasryo/CreditCard-fraud-detection-ML", "model_rf_cal.joblib")
rf_cal = joblib.load(path)
