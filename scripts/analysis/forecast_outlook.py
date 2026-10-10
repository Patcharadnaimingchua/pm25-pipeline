"""Seasonal PM2.5 outlook per province + tomorrow ridge model, from mart export v_station_day.csv.

    python scripts/analysis/forecast_outlook.py provinces.geojson data/export_csv/v_station_day.csv out_dir
Writes out_dir/fc.json (province x month climatology, national, backtest) and out_dir/tm.json (ridge coefficients).
Backtest: climatology from 2022-2024 vs 2025 actuals. Nothing is imputed; months with <15 station-days are dropped.
"""
import json, sys, numpy as np, pandas as pd
from shapely.geometry import shape, Point
from shapely.strtree import STRtree
g=json.load(open(sys.argv[1]))
polys=[shape(f["geometry"]) for f in g["features"]]; props=[f["properties"] for f in g["features"]]
tree=STRtree(polys)
d=pd.read_csv(sys.argv[2],parse_dates=["full_date"])
d=d[d.year<=2025].dropna(subset=["pm25_mean"])
st=d.groupby("station_id").agg(lat=("latitude","first"),lon=("longitude","first")).reset_index()
def find(la,lo):
    p=Point(lo,la)
    for i in tree.query(p):
        if polys[i].covers(p): return i
    i=int(tree.nearest(p)); return i if polys[i].distance(p)<0.1 else None
st["pi"]=[find(a,b) for a,b in zip(st.lat,st.lon)]
st["th"]=[props[i]["pro_th"] for i in st.pi]
d=d.merge(st[["station_id","th"]],on="station_id")
d["exc"]=(d.pm25_mean>37.5).astype(float)
# province-month-year means
pmy=d.groupby(["th","year","month"]).agg(pm=("pm25_mean","mean"),n=("pm25_mean","size"),nst=("station_id","nunique")).reset_index()
pmy=pmy[pmy.n>=15*pmy.nst.clip(upper=1)]   # at least 15 station-days in that province-month
def clim(df):
    a=df.groupby(["th","month"]).pm.agg(mean="mean",lo="min",hi="max",yrs="count").reset_index()
    e=d[d.year.isin(df.year.unique())].groupby(["th","month"]).exc.mean().rename("pexc").reset_index()
    return a.merge(e,on=["th","month"],how="left")
# backtest: climatology 2022-2024 -> 2025
tr=pmy[pmy.year<=2024]; te=pmy[pmy.year==2025]
c=clim(tr); b=te.merge(c,on=["th","month"],suffixes=("","_c")); 
b["err"]=(b.pm-b["mean"]).abs()
naive=te.merge(pmy[pmy.year==2024][["th","month","pm"]],on=["th","month"],suffixes=("","_n")); naive["err"]=(naive.pm-naive.pm_n).abs()
inband=((b.pm>=b.lo-3)&(b.pm<=b.hi+3)).mean()
r2=1-((b.pm-b["mean"])**2).sum()/((b.pm-b.pm.mean())**2).sum()
# does it flag the high-risk months? predicted mean>30 vs actual>30
hi=(b["mean"]>30); ac=(b.pm>30)
print("n",len(b),"MAE clim",b.err.mean().round(2),"MAE naive",naive.err.mean().round(2),"R2",round(r2,3),"inband",round(inband,3))
print("high month precision",round((hi&ac).sum()/max(hi.sum(),1),3),"recall",round((hi&ac).sum()/max(ac.sum(),1),3))
bt=dict(n=int(len(b)),mae=round(float(b.err.mean()),2),mae_naive=round(float(naive.err.mean()),2),r2=round(float(r2),3),
  prec=round(float((hi&ac).sum()/max(hi.sum(),1)),2),rec=round(float((hi&ac).sum()/max(ac.sum(),1)),2))
# final climatology 2022-2025
cf=clim(pmy)
provs=sorted(cf.th.unique())
out={}
for th in provs:
    x=cf[cf.th==th].set_index("month")
    out[th]=[None if m not in x.index else [round(x.loc[m,"mean"],1),round(x.loc[m,"lo"],1),round(x.loc[m,"hi"],1),int(x.loc[m,"yrs"]),round(float(x.loc[m,"pexc"])*100)] for m in range(1,13)]
# national = mean of province means per month
nat=[]
for m in range(1,13):
    x=cf[cf.month==m]; ex=d[d.month==m].groupby("th").exc.mean()
    nat.append([round(x["mean"].mean(),1),round(x.lo.mean(),1),round(x.hi.mean(),1),int(x.yrs.max()),round(float(ex.mean())*100)])
json.dump(dict(prov=out,nat=nat,bt=bt),open(sys.argv[3]+"/fc.json","w"),ensure_ascii=False,separators=(",",":"))
print(nat)
# tomorrow model: ridge
from sklearn.linear_model import Ridge
d2=d.sort_values(["station_id","full_date"])
nx=d2[["station_id","full_date","pm25_mean"]].copy(); nx["full_date"]-=pd.Timedelta(days=1); nx=nx.rename(columns={"pm25_mean":"y"})
m=d2.merge(nx,on=["station_id","full_date"])
ang=2*np.pi*(m.month-1)/12; m["ms"]=np.sin(ang); m["mc"]=np.cos(ang)
m["lh"]=np.log1p(m.hotspots_200km)
F=["pm25_mean","rh_mean","wind_speed_mean","lh","precip_sum","ms","mc"]
m=m.dropna(subset=F+["y"])
trn,tst=m[m.year<=2024],m[m.year==2025]
mu=trn[F].mean(); sd=trn[F].std()
rg=Ridge(alpha=1.0).fit((trn[F]-mu)/sd,trn.y)
p=np.clip(rg.predict((tst[F]-mu)/sd),0,None)
mae=float(np.abs(tst.y-p).mean()); mae0=float(np.abs(tst.y-tst.pm25_mean).mean())
print("ridge",len(tst),round(mae,2),"persist",round(mae0,2))
lo=trn[F].quantile(.01); hi_=trn[F].quantile(.99)
json.dump(dict(F=F,mu=[round(float(x),4) for x in mu],sd=[round(float(x),4) for x in sd],coef=[round(float(x),4) for x in rg.coef_],b=round(float(rg.intercept_),4),
  mae=round(mae,2),mae0=round(mae0,2),n=int(len(tst)),
  rng={"pm":[0,round(float(trn.pm25_mean.quantile(.99)),0)],"rh":[round(float(lo.rh_mean)),round(float(hi_.rh_mean))],"wind":[0,round(float(hi_.wind_speed_mean))],"hs":[0,round(float(trn.hotspots_200km.quantile(.99)))]}),
  open(sys.argv[3]+"/tm.json","w"),separators=(",",":"))
print(open(sys.argv[3]+"/tm.json").read())
