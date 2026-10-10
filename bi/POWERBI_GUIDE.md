# คู่มือสร้างรายงาน Power BI (PM2.5 Thailand)

ทำบน Power BI Desktop (Windows) ใช้ไฟล์ CSV ที่ export จากฐานข้อมูลโดยตรง ไม่ต้องต่อเครือข่ายกับเครื่องที่รัน Docker

## 0. ไฟล์ที่ต้องมี

โฟลเดอร์ `export_csv` (10 ไฟล์ ได้จาก `bash scripts/export_mart_csv.sh`) และไฟล์จากโฟลเดอร์ `bi/`:
`station_province.csv` (สถานี → จังหวัด) และ `pm25_theme.json` (ธีมสี)

## 1. นำเข้าข้อมูล

Home → Get Data → Text/CSV แล้วเลือก File Origin เป็น **65001: Unicode (UTF-8)** กด Transform Data เพื่อตั้งชนิดคอลัมน์ก่อน Load

| ไฟล์ | ใช้ทำอะไร | ตั้งชนิดคอลัมน์ |
| --- | --- | --- |
| `v_station_day.csv` | ตารางหลัก (179,647 แถว: สถานี-วัน) | `full_date` = Date, ตัวเลข = Decimal Number |
| `station_province.csv` | บอกว่าสถานีอยู่จังหวัดไหน (234 แถว) | `latitude`, `longitude` = Decimal Number, ตั้ง Data Category ของสองคอลัมน์เป็น Latitude / Longitude |
| `v_monthly_pm25.csv` | สรุปรายเดือน | `year`, `month` = Whole Number |
| `v_pm25_coverage_month.csv` | ความครบของข้อมูล | ตัวเลขทั้งหมด |
| `v_hotspot_daily.csv` | จุดความร้อนรายวัน | `full_date` = Date |
| `v_data_quality.csv` | ผลรันไปป์ไลน์ | `started_at` = Date/Time |
| `pm25_threshold.csv` | เส้นเกณฑ์ WHO / ไทย | `value_ugm3` = Decimal Number |

ไม่ต้องนำเข้า `v_pm25_vs_hotspot.csv`, `v_station_hour_latest.csv`, `v_modeled_pm25.csv`, `v_row_counts.csv` ถ้าไม่จำเป็น (ข้อมูลซ้ำกับตารางหลัก หรือมีแค่วันเดียว)

คอลัมน์ `exceeds_th_standard` และ `is_burning_season` เป็นข้อความ `t` / `f` (ไม่ใช่ True/False) ให้ใช้เงื่อนไข `= "t"` ใน DAX

## 2. ตั้งความสัมพันธ์ (Model view)

ลาก `station_province[station_id]` ไปที่ `v_station_day[station_id]` ให้เป็น **One-to-many** (ทิศทางกรองจาก station_province ไป v_station_day)

สร้างตารางวันที่ (New table):

```
Calendar = CALENDAR(DATE(2022,1,1), DATE(2025,12,31))
```

ลาก `Calendar[Date]` ไปที่ `v_station_day[full_date]` (One-to-many) แล้วเพิ่มคอลัมน์ `Year = YEAR([Date])` และ `Month = MONTH([Date])` ในตาราง Calendar

## 3. Measures (New measure ในตาราง v_station_day)

```
PM2.5 เฉลี่ย = AVERAGE(v_station_day[pm25_mean])

วัน-สถานี = COUNTROWS(v_station_day)

วันที่เกินเกณฑ์ไทย = CALCULATE([วัน-สถานี], v_station_day[exceeds_th_standard] = "t")

% วันที่เกินเกณฑ์ไทย = DIVIDE([วันที่เกินเกณฑ์ไทย], [วัน-สถานี])

จุดความร้อนเฉลี่ย 100 กม. = AVERAGE(v_station_day[hotspots_100km])

จำนวนสถานี = DISTINCTCOUNT(v_station_day[station_id])

เกณฑ์ WHO = 15
เกณฑ์ไทย = 37.5
```

ตั้งรูปแบบ `PM2.5 เฉลี่ย` เป็น 1 ตำแหน่งทศนิยม และ `% วันที่เกินเกณฑ์ไทย` เป็นเปอร์เซ็นต์

## 4. ธีมสี

View → Themes → Browse for themes → เลือก `pm25_theme.json`

สีเข้มคือฝุ่นมาก: เขียวอ่อน (≤15) → เหลือง (15–25) → ส้ม (25–37.5) → แดง (>37.5) ใช้หลักเดียวกันทุกหน้า

## 5. หน้ารายงาน (4 หน้า ไม่ต้องซับซ้อน)

**หน้า 1: ภาพรวม**
- การ์ด 4 ใบ: `PM2.5 เฉลี่ย`, `% วันที่เกินเกณฑ์ไทย`, `จำนวนสถานี`, `วัน-สถานี`
- Line chart: แกน X = `Calendar[Year]` + `Calendar[Month]`, ค่า = `PM2.5 เฉลี่ย` (ใส่ Constant line ที่ 15 และ 37.5 ใน Analytics pane)
- Slicer: ปี

**หน้า 2: ตามจังหวัด**
- Map (Bubble): ละติจูด/ลองจิจูดจาก `station_province`, ขนาดและสี = `PM2.5 เฉลี่ย` ตั้ง Conditional formatting สีตามค่า
- Bar chart: แกน = `station_province[province_th]`, ค่า = `PM2.5 เฉลี่ย` เรียงมากไปน้อย ใส่ตัวกรอง `stations_in_province >= 2` เพื่อไม่ให้จังหวัดที่มีเครื่องวัดเครื่องเดียวอยู่ในอันดับ
- Slicer: ปี, เดือน

**หน้า 3: ฝุ่นกับจุดความร้อน**
- Scatter: X = `จุดความร้อนเฉลี่ย 100 กม.` (หรือ `hotspots_100km` แบบไม่รวม), Y = `PM2.5 เฉลี่ย`, Details = `station_province[province_th]`
- สองกราฟวางซ้อนกันแกนเวลาเดียวกัน: เส้น PM2.5 รายเดือนจาก `v_monthly_pm25` (แกน X = `year`, `month`) และแท่งจุดความร้อนรายเดือนจาก `v_hotspot_daily` (ค่า = ผลรวม `hotspots`, แกน X = `year`, `month`) ไม่ต้องสร้างความสัมพันธ์ข้ามตารางเพราะวางเป็นสองกราฟแยก
- ข้อความสั้นๆ ว่า "สัมพันธ์กัน ไม่ได้พิสูจน์สาเหตุ"

**หน้า 4: คุณภาพข้อมูล**
- Column chart จาก `v_pm25_coverage_month`: แกน X = year-month, ค่า = `coverage_pct` (เห็นเดือนที่ข้อมูลขาด เช่น ก.ค.–ส.ค. 2022, ก.ค. 2025)
- Table จาก `v_data_quality`: `task_id`, `records_in`, `records_out`, `records_duplicate`, `records_quarantined`, `status`
- กล่องข้อความ: "OpenAQ แถวเข้า 15.4 ล้าน ออก 7.7 ล้านเพราะไฟล์ดิบซ้ำสองชุด ตัดซ้ำตามคีย์ธุรกิจแล้ว"

## 6. ข้อความที่ต้องใส่ในรายงาน

- แหล่งข้อมูล: OpenAQ (Air4Thai/กรมควบคุมมลพิษ), NASA FIRMS, Open-Meteo (ERA5), GISTDA
- เกณฑ์: WHO 15 และมาตรฐานไทย 37.5 µg/m³ (ค่าเฉลี่ย 24 ชั่วโมง)
- ข้อจำกัด: เดือน ก.ค.–ส.ค. 2022 และ ก.ค. 2025 แทบไม่มีข้อมูลในไฟล์ต้นทาง (ไม่เติมค่า), จังหวัดมาจากการจับพิกัดสถานีกับขอบเขตจังหวัด (geoBoundaries / OpenStreetMap, ODbL), หลายจังหวัดมีเครื่องวัดเครื่องเดียว
- ความสัมพันธ์ระหว่างจุดความร้อนกับ PM2.5 ไม่ได้พิสูจน์ว่าไฟเป็นสาเหตุเพียงอย่างเดียว

## 7. ส่งงาน

File → Save as `.pbix` และ Export เป็น PDF (File → Export → Export to PDF) ถ้าต้องส่งแบบดูอย่างเดียว

หมายเหตุ: ไฟล์นี้ใช้ข้อมูลที่ export ไว้ (ถึง ธ.ค. 2025) ถ้ารัน pipeline ใหม่แล้วต้อง export CSV ใหม่ และกด Refresh ใน Power BI
