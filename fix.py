import re
from pathlib import Path

target_file = Path(r"c:\Users\비큐리오\PycharmProjects\main.py")
content = target_file.read_text(encoding="utf-8")

# 1. 전역 변수 child_env 추가
if "child_env = os.environ.copy()" not in content:
    insertion_target = "DB_DIR = BASE_DIR / \"src\" / \"database\" / \"DBIntegration\""
    insertion_string = """DB_DIR = BASE_DIR / "src" / "database" / "DBIntegration"

# 서브프로세스 환경 변수 공유 (PYTHONPATH에 프로젝트 루트 주입)
child_env = os.environ.copy()
child_env["PYTHONPATH"] = str(BASE_DIR)
"""
    content = content.replace(insertion_target, insertion_string)

# 2. 모든 subprocess.run에 env=child_env 추가
def replacer(match):
    inner = match.group(1)
    if "env=child_env" in inner:
        return match.group(0) # 이미 적용됨
    return f"subprocess.run({inner}, env=child_env)"

new_content = re.sub(r'subprocess\.run\((.*?)\)', replacer, content)

target_file.write_text(new_content, encoding="utf-8")
print("Successfully updated main.py with env=child_env")
