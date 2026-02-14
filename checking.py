from model import rf_cal

# Check what features the model expects
if hasattr(rf_cal, 'feature_names_in_'):
    print("Model expects these features:")
    print(rf_cal.feature_names_in_)
    print(f"\nTotal features: {len(rf_cal.feature_names_in_)}")
else:
    print(f"Model expects {rf_cal.n_features_in_} features")