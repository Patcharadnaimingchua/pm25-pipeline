# Data sources and attribution

Show the relevant lines on every dashboard page and in the report. Licences checked 2026-10-02.

| Source | Use in this project | Licence / terms | Attribution text |
| --- | --- | --- | --- |
| OpenAQ (S3 archive + API v3) | Hourly station measurements (Air4Thai and other providers) | Varies by provider; attribution to OpenAQ **and** the original provider is required — https://docs.openaq.org/about/terms | Air quality data: OpenAQ (openaq.org); original data: Pollution Control Department, Thailand (Air4Thai) |
| NASA FIRMS | VIIRS active fire / hotspot detections | NASA open data policy — https://firms.modaps.eosdis.nasa.gov/api/area/ | Active fire data: NASA FIRMS (firms.modaps.eosdis.nasa.gov), LANCE/ESDIS. *Check the exact acknowledgement wording requested on the FIRMS site before publishing.* |
| Open-Meteo Historical Weather API | ERA5 hourly weather at station grid cells | CC BY 4.0; free for non-commercial use within published limits — https://open-meteo.com/en/terms | Weather data by Open-Meteo.com (CC BY 4.0), based on ERA5 (Copernicus Climate Change Service) |
| GISTDA ระบบเช็คฝุ่น | Hourly modeled PM2.5 by province/district (collected by this project) | Licence not specified on the GISTDA Open Data catalog — educational use, credit, no raw redistribution; permission requested | ค่า PM2.5 ประมาณการ: GISTDA ระบบเช็คฝุ่น (pm25.gistda.or.th) |
| Thailand province boundaries (GeoJSON) | จับพิกัดสถานีเข้าจังหวัด และวาดแผนที่ในหน้าเว็บ | geoBoundaries THA ADM1 (ที่มา OpenStreetMap) ODbL 1.0 — ต้องระบุที่มาและหากเผยแพร่ฐานข้อมูลที่ดัดแปลงต้องใช้ ODbL เดียวกัน — https://www.geoboundaries.org (ตรวจ 2026-10-03 จาก metadata ในไฟล์) | ขอบเขตจังหวัด: geoBoundaries (ข้อมูล © ผู้ร่วมสร้าง OpenStreetMap, ODbL 1.0) |

Notes for charts: GISTDA values are model estimates; ERA5 is reanalysis on a grid; relationships shown are associations, not causes.

มาตรฐาน PM2.5 ไทย: ค่าเฉลี่ย 24 ชั่วโมงปรับจาก 50 เป็น 37.5 µg/m³ ตามมติคณะกรรมการสิ่งแวดล้อมแห่งชาติ พ.ศ. 2565 (ยืนยันจากหัวข้อข่าวสำนักงานนโยบายและแผนทรัพยากรธรรมชาติและสิ่งแวดล้อม onep.go.th ตรวจ 2026-10-03 ยังไม่ได้อ่านประกาศฉบับเต็มและวันมีผลบังคับใช้)
