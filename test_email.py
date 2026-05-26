import imaplib
import email
import json
import re
from email.header import decode_header
from pathlib import Path

def get_credentials():
    config_path = Path("naverworks_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
        return config.get("email"), config.get("password")

def decode_mime_words(s):
    if not s:
        return ""
    decoded_words = decode_header(s)
    result = []
    for word, charset in decoded_words:
        if isinstance(word, bytes):
            result.append(word.decode(charset or 'utf-8', errors='replace'))
        else:
            result.append(word)
    return "".join(result)

def test_email():
    user_email, user_password = get_credentials()
    imap_server = "imap.worksmobile.com"
    
    mail = imaplib.IMAP4_SSL(imap_server, 993)
    mail.login(user_email, user_password)
    mail.select("inbox")
    
    status, messages = mail.search(None, 'FROM', '"cmrhee@becurio.com"')
    mail_ids = messages[0].split()
    
    target_subject_part = "[서울-20260523]경주결과 Report"
    
    for i in reversed(mail_ids):
        res, msg_data = mail.fetch(i, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject = decode_mime_words(msg["Subject"])
                
                if subject and target_subject_part in subject:
                    print(f"\n=========================================")
                    print(f"찾은 메일 제목: {subject}")
                    print(f"=========================================\n")
                    
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        disposition = str(part.get('Content-Disposition'))
                        
                        print(f"--- 파트 발견 ---")
                        print(f"Content-Type: {content_type}")
                        print(f"Content-Disposition: {disposition}")
                        
                        if content_type == "text/plain":
                            body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                            print(f"\n[텍스트 본문 내용 일부]\n{body[:500]}")
                        
                        elif content_type == "text/html":
                            body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                            print(f"\n[HTML 본문 내용 일부]\n{body[:500]}")
                            
                        elif "attachment" in disposition:
                            filename = part.get_filename()
                            if filename:
                                filename = decode_mime_words(filename)
                            print(f"\n[첨부파일 발견] 파일명: {filename}")
                    
                    return

    print("해당 메일을 찾지 못했습니다.")

if __name__ == "__main__":
    test_email()
