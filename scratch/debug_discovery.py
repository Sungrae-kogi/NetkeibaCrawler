import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.netkeiba.com/"
}

url = "https://race.netkeiba.com/top/race_list_sub.html?kaisai_date=20260404"
r = requests.get(url, headers=HEADERS)
r.encoding = "EUC-JP"
soup = BeautifulSoup(r.text, "html.parser")

titles = soup.select(".RaceList_DataTitle")
print(f"Total titles: {len(titles)}")

for i, title in enumerate(titles):
    print(f"\nTitle {i}: {title.get_text(strip=True)}")
    # Find all siblings until the next title
    current = title.next_sibling
    found_link = None
    while current:
        if hasattr(current, 'name') and current.name == "p" and "RaceList_DataTitle" in current.get('class', []):
            break # Reached next title
        
        if hasattr(current, 'select_one'):
            link_elem = current.select_one("a[href*='race_id=']")
            if link_elem:
                found_link = link_elem.get('href')
                break
        
        current = current.next_sibling
    
    print(f" -> Found 1R link: {found_link}")
