"""Next-day PM2.5 per station from today's PM2.5, weather and nearby hotspots.

Reads the mart export (data/export_csv/v_station_day.csv), so it analyses exactly what the warehouse holds.
Time-based split: train 2022-2024, test 2025. Compared against a "tomorrow = today" persistence baseline.
Writes analysis_results.json (feature importance, correlations, metrics).

    python scripts/analysis/pm25_model.py data/export_csv/v_station_day.csv analysis_results.json
"""
import json
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, r2_score

src, dst = sys.argv[1], sys.argv[2]
d = pd.read_csv(src, parse_dates=["full_date"])
d = d.dropna(subset=["pm25_mean"]).sort_values(["station_id", "full_date"])

FEATURES = ["pm25_mean", "temp_mean", "rh_mean", "wind_speed_mean", "precip_sum", "blh_mean",
            "hotspots_50km", "hotspots_100km", "hotspots_200km", "frp_sum_100km", "month_sin", "month_cos"]
LABELS = {"pm25_mean": "PM2.5 วันนี้", "temp_mean": "อุณหภูมิ", "rh_mean": "ความชื้นสัมพัทธ์", "wind_speed_mean": "ความเร็วลม",
          "precip_sum": "ฝน", "blh_mean": "ความสูงชั้นบรรยากาศผสม", "hotspots_50km": "จุดความร้อน 50 กม.",
          "hotspots_100km": "จุดความร้อน 100 กม.", "hotspots_200km": "จุดความร้อน 200 กม.",
          "frp_sum_100km": "พลังงานไฟ (FRP) 100 กม.", "month_sin": "ฤดูกาล (sin)", "month_cos": "ฤดูกาล (cos)"}

# pair each station-day with the next calendar day only (no gap bridging)
nxt = d[["station_id", "full_date", "pm25_mean"]].copy()
nxt["full_date"] = nxt["full_date"] - pd.Timedelta(days=1)
nxt = nxt.rename(columns={"pm25_mean": "target"})
m = d.merge(nxt, on=["station_id", "full_date"])
ang = 2 * np.pi * (m["month"] - 1) / 12
m["month_sin"], m["month_cos"] = np.sin(ang), np.cos(ang)
train, test = m[m.year <= 2024], m[m.year == 2025]

def evaluate(y, p):
    return {"mae": round(float(mean_absolute_error(y, p)), 2), "r2": round(float(r2_score(y, p)), 3)}

model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.06, random_state=0)
model.fit(train[FEATURES], train["target"])        # handles missing weather natively; nothing is imputed
pred = model.predict(test[FEATURES])
base = evaluate(test["target"], test["pm25_mean"])
mod = evaluate(test["target"], pred)

sample = test.sample(min(len(test), 6000), random_state=0)
imp = permutation_importance(model, sample[FEATURES], sample["target"], n_repeats=5, random_state=0,
                             scoring="neg_mean_absolute_error")
importance = sorted(({"f": LABELS[f], "v": round(float(v), 3)} for f, v in zip(FEATURES, imp.importances_mean)),
                    key=lambda r: -r["v"])

cors = []
for f in ["hotspots_50km", "hotspots_100km", "hotspots_200km", "frp_sum_100km", "temp_mean", "rh_mean",
          "wind_speed_mean", "precip_sum", "blh_mean"]:
    x = d[[f, "pm25_mean"]].dropna()
    cors.append({"f": LABELS[f], "r": round(float(x[f].corr(x["pm25_mean"], method="spearman")), 2), "n": int(len(x))})
cors.sort(key=lambda r: -abs(r["r"]))

hit = test[test["target"] > 37.5]
res = {"train_rows": int(len(train)), "test_rows": int(len(test)), "baseline": base, "model": mod,
       "exceed_days_in_test": int(len(hit)),
       "exceed_recall_model": round(float((model.predict(hit[FEATURES]) > 37.5).mean()), 3) if len(hit) else None,
       "exceed_recall_baseline": round(float((hit["pm25_mean"] > 37.5).mean()), 3) if len(hit) else None,
       "importance": importance, "correlations": cors}
json.dump(res, open(dst, "w"), ensure_ascii=False, indent=1)
print(json.dumps(res, ensure_ascii=False, indent=1))
