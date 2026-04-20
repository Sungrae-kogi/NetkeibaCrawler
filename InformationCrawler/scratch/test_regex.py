import re
from datetime import datetime

place = "土曜阪神6R"
details = "2番 エイコーンドリーム (4/18 12:43)"

m_place = re.match(r"^(.)曜(.+?)(\d+)R", place)
if m_place:
    rcday = m_place.group(1)
    meet = m_place.group(2)
    rcno = m_place.group(3)
    print(f"RCDAY={rcday}, MEET={meet}, RCNO={rcno}")

m_details = re.search(r"(\d+)番\s+(.+?)\s+\((\d+)/(\d+)", details.replace('\xa0', ' '))
if m_details:
    chulno = m_details.group(1)
    hrname = m_details.group(2).strip()
    month = int(m_details.group(3))
    day = int(m_details.group(4))
    year = datetime.now().year
    rcdate = f"{year}{month:02d}{day:02d}"
    print(f"CHULNO={chulno}, HRNAME={hrname}, RCDATE={rcdate}")
