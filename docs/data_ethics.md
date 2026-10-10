# Data ethics: ดึงข้อมูลให้ได้มากที่สุด โดยไม่เป็นภาระและไม่ละเมิดใคร

เป้าหมายคือได้ข้อมูลจริงมากที่สุดที่คำถามวิเคราะห์ใช้ได้ (≥ 1M records ตาม hard requirement) โดยแต่ละ request
ตรวจสอบย้อนหลังได้ว่าอยู่ในกติกาของเจ้าของข้อมูล กติกาเหล่านี้ไม่ได้อยู่แค่ในเอกสาร แต่ code บังคับใช้จริง
(`src/pm25/ingestion/polite.py` + `config/source_policies.yaml`) และ test ตรวจอยู่ (`tests/unit/test_polite.py`)

## หลัก 8 ข้อ

| # | หลัก | code ทำอะไร |
| --- | --- | --- |
| 1 | **Bulk ก่อน API, ไม่ scrape หน้าเว็บ** | OpenAQ ดึงจาก S3 open-data archive ที่ OpenAQ จัดไว้ให้ดาวน์โหลดจำนวนมาก; Air4Thai ถูกปิดใน policy (robots.txt ห้าม + ไม่มี API doc) จึงใช้ข้อมูลผ่าน OpenAQ แทน |
| 2 | **อยู่ต่ำกว่าทุก limit ที่ประกาศ (≤ 80%)** | rolling budget หลายช่วงพร้อมกัน เช่น Open-Meteo 480/นาที, 4,000/ชม., 8,000/วัน (ประกาศ 600 / 5,000 / 10,000); FIRMS 4,000 transactions/10 นาที (ประกาศ 5,000) โดยนับ request 5 วันเป็น 5 transactions ตามกติกา FIRMS |
| 3 | **บอกว่าเราเป็นใคร** | client ไม่ยอมทำงานถ้า `HTTP_USER_AGENT` ไม่มีอีเมลติดต่อจริง |
| 4 | **หยุดเมื่อเขาขอให้หยุด** | 429/503 อ่าน `Retry-After` แล้วรอตามนั้น; backoff แบบ exponential + jitter; circuit breaker หยุดเรียก host ที่ล้มติดกัน 5 ครั้งเป็นเวลา 15 นาที |
| 5 | **robots.txt** | host ทั่วไป: เชื่อฟัง (RFC 9309: 4xx = อนุญาต, 5xx/ติดต่อไม่ได้ = ห้าม); API ที่มีเอกสารให้ใช้แบบ programmatic: อ่าน robots.txt เสมอ และบันทึกเหตุผลที่ทำตาม API terms ลง ledger (ดูหัวข้อ Open-Meteo) |
| 6 | **ไม่ดึงซ้ำ** | ทุกสคริปต์ resumable (มีไฟล์แล้ว = ข้าม), เขียนไฟล์แบบ atomic (`.part` แล้ว rename); `push` เข้า lake ข้ามไฟล์ที่ sha256 เคย push แล้ว |
| 7 | **เอาเท่าที่ใช้** | ERA5 ดึงครั้งเดียวต่อ grid cell ไม่ใช่ต่อสถานี (สถานี กทม. หลายสถานีอยู่ cell เดียวกัน); ไม่ดึง GISTDA ระดับตำบล; ไม่ดึง grid อากาศทั้งประเทศ |
| 8 | **เครดิตและ license ติดไปกับข้อมูล** | `ATTRIBUTION.md` + ทุก host ใน policy มี licence/attribution; dashboard ทุกหน้ามี footnote แหล่งข้อมูล |

ทุก request ถูกบันทึกลง `data/ledger/requests-YYYY-MM.jsonl` (ปิดบัง API key อัตโนมัติ) และ
`python scripts/backfill/run.py report` สรุป peak ต่อนาที/ชั่วโมง/วัน ต่อ host เพื่อเทียบกับ limit ที่ประกาศ —
ตารางนี้คือหลักฐานใน report ว่าเราดึงอย่างมีจรรยาบรรณ

## สิ่งที่เราตัดสินใจไม่ทำ (และเพราะอะไร)

- **ไม่ scrape Air4Thai** — robots.txt ของ air4thai.pcd.go.th ปฏิเสธ automated access และไม่มี API ที่มีเอกสาร
- **ไม่ crawl GISTDA ระดับตำบล** — จะต้องยิง ~7,255 request/วันไปยัง service ของรัฐที่ไม่ได้ประกาศ limit และไม่มีคำถามวิเคราะห์ไหนต้องใช้ grain นั้น
- **ไม่ปั่นจำนวน record** — ไม่นับไฟล์ซ้ำ, ไม่นับ ERA5 ซ้ำต่อสถานี, ไม่สร้าง synthetic data, ไม่ดึงช่วงเวลาที่ไม่มี PM2.5 จริงให้ join
- **ไม่แชร์ raw ของ GISTDA ต่อ** — license ไม่ระบุ; ใช้เพื่อการศึกษาพร้อมให้เครดิต และควรอีเมลขออนุญาต (ร่างด้านล่าง)
- **ไม่ปล่อย DAG ทิ้งไว้เมื่อจบโปรเจกต์** — OpenAQ terms ห้ามปล่อย request ทำงานต่อเนื่องเมื่อไม่ต้องการข้อมูลแล้ว: pause ทุก DAG หลังส่งงาน

## กรณี Open-Meteo กับ robots.txt (ตัดสินใจอย่างโปร่งใส)

robots.txt ของ `archive-api.open-meteo.com` ปฏิเสธ automated agents (ตรวจ 2 ต.ค. 2026) ขณะที่ terms ของ
Open-Meteo เปิดให้ใช้ API ฟรีแบบ programmatic สำหรับงานไม่แสวงกำไร/การศึกษาภายใต้ limit ที่ประกาศ เราตีความว่า
robots.txt บน API host มีไว้กันเว็บ crawler ไม่ใช่ API client ที่ทำตาม terms จึงตั้ง policy เป็น `api_terms`
และบันทึก verdict ของ robots.txt ลง ledger ทุกครั้งที่เริ่ม process

ถ้าทีมหรืออาจารย์ต้องการตีความแบบเข้มกว่านี้ ทางเลือกคือดึง ERA5 จากต้นทางคือ Copernicus Climate Data Store
(ต้องสมัครบัญชีและยอมรับ licence) — เปลี่ยนเฉพาะ client ของ weather, ส่วนอื่นของ pipeline เหมือนเดิม

## แผนดึงข้อมูลให้ได้มาก (window 2022-01-01 → 2025-12-31)

| Tier | สิ่งที่ดึง | ใช้ตอบ | ภาระต่อ source (ESTIMATED) | ทำเมื่อ |
| --- | --- | --- | --- | --- |
| A | OpenAQ: สถานี Air4Thai ทุก pollutant | Q1–Q9 | list ≈ สถานี × 48 เดือน + ≈ 1 ไฟล์/สถานี/วัน (~0.3 KB/ไฟล์) จาก S3 | Week 1 |
| B | FIRMS: VIIRS SNPP SP (+ NOAA-20 SP, NOAA-21 NRT) bbox ไทย | Q6 | 293 requests = 1,461 transactions ต่อดาวเทียม | Week 1–2 |
| C | ERA5 ต่อ grid cell ของสถานี | Q4, Q5 | 1 call ต่อ cell ต่อปี (≈ 4 × จำนวน cell) | Week 2 |
| D | GISTDA ระดับอำเภอ ทุกชั่วโมงไปข้างหน้า | Q9 | 78 calls/ชม. + 78 calls/วัน | เปิดตั้งแต่วันแรก |
| E | OpenAQ provider อื่นในไทย (low-cost) แยก `quality_tier` | หน้าเปรียบเทียบ reference vs low-cost | เหมือน Tier A | ถ้า Gate G1 ยังไม่ผ่าน หรือมีหน้า dashboard ใช้จริง |
| F | FIRMS bbox ภูมิภาค (เมียนมา ลาว กัมพูชา) | Q7 | เหมือน Tier B | Week 3 |

ทุก tier เป็นข้อมูลจริงที่มีคำถามวิเคราะห์รองรับ — Tier E และ F นับเข้า record count ได้ก็ต่อเมื่อมี visual/คำถามที่ใช้มันจริง

## ร่างอีเมลขออนุญาต GISTDA (ส่งถึง InnoTech@gistda.or.th)

> เรียน ทีมระบบเช็คฝุ่น สำนักงานพัฒนาเทคโนโลยีอวกาศและภูมิสารสนเทศ
>
> กลุ่มนักศึกษาปี 4 สาขา IT ([มหาวิทยาลัย]) ขออนุญาตใช้ข้อมูล PM2.5 รายจังหวัด/รายอำเภอจาก API ของระบบเช็คฝุ่น
> (pm25.gistda.or.th/rest) ในโปรเจกต์รายวิชา Big Data ซึ่งจะเรียก API ไม่เกินชั่วโมงละ 80 ครั้ง ระบุตัวตนใน
> User-Agent และให้เครดิต GISTDA ในทุกผลงาน ข้อมูลใช้เพื่อการศึกษาเท่านั้น ไม่เผยแพร่ข้อมูลดิบต่อ
> หากมีเงื่อนไขการใช้งานหรือ license ที่ควรปฏิบัติตาม รบกวนแจ้งด้วยครับ/ค่ะ
>
> ขอบคุณครับ/ค่ะ — [ชื่อ, อีเมล, อาจารย์ที่ปรึกษา]

## บันทึกผลตรวจ robots.txt ของ Open-Meteo (ตรวจ 2026-10-03)

ตัวเลข `robots_disallow_events = 6` ในรายงาน request ledger ไม่ใช่การดึงข้อมูลที่ถูกบล็อก
มันคือบันทึก 6 ครั้ง (ครั้งละหนึ่งรอบของ task ที่เริ่มเรียก API) ว่า `robots.txt` ของ `archive-api.open-meteo.com` ระบุ disallow
สำหรับ agent อัตโนมัติ ในขณะที่ข้อกำหนดของ Open-Meteo เสนอ API นี้ให้ใช้ฟรีสำหรับงานไม่แสวงหากำไรและงานวิจัยภายในขีดจำกัดที่เผยแพร่
เราเลือกตามข้อกำหนดของ API (`robots: api_terms` ใน `config/source_policies.yaml`) พร้อมเหตุผลที่บันทึกไว้
และเก็บคำตัดสินนี้ในทุกรอบของ ledger

ผลที่เกิดขึ้นจริง: เรียก API 683 ครั้ง (นับน้ำหนัก 5,378) โดยแต่ละชั่วโมงไม่เกินงบ 4,000 และต่อวันไม่เกิน 8,000
ได้ HTTP 429 รวม 113 ครั้ง ระบบรอแล้วลองใหม่ตามงบ ไม่มีการเร่งหลบขีดจำกัด

ความเสี่ยงที่ต้องแจ้งอาจารย์: นี่คือการตัดสินใจที่ขัดกับ robots.txt แต่สอดคล้องกับข้อกำหนด API ที่เผยแพร่
ทางเลือกที่เข้มงวดกว่าคือ ERA5 จาก Copernicus Climate Data Store (ต้องสมัครบัญชีและยอมรับ licence) ซึ่งเปลี่ยนเฉพาะ client ของ weather
