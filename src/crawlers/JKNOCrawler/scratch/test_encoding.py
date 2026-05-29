import requests

URL = "https://db.netkeiba.com/jockey/01066/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

r = requests.get(URL, headers=HEADERS)
print(f"Original Encoding: {r.encoding}")
print(f"Apparent Encoding: {r.apparent_encoding}")

# Test with apparent_encoding
r.encoding = r.apparent_encoding
text_apparent = r.text[:500]
print("\n--- Content with apparent_encoding ---")
print(text_apparent)

# Test with EUC-JP
r.encoding = "EUC-JP"
text_euc = r.text[:500]
print("\n--- Content with EUC-JP ---")
print(text_euc)
