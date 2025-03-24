import asyncio
import re
import email
from pathlib import Path
from email import policy
from email.header import decode_header
from functools import lru_cache

import aiofiles
import html2text

from docling.document_converter import DocumentConverter

def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

@lru_cache(maxsize=1024)
def decode_header_value(header_value) -> str:
    if not header_value:
        return ""

    decoded_parts = decode_header(header_value)
    return "".join(
        part.decode(encoding if encoding else "utf-8", errors="replace")
        if isinstance(part, bytes)
        else part
        for part, encoding in decoded_parts
    )

class EmailHandler:
    def __init__(self, msg_id: str, raw_email: bytes, base_path: Path):
        self.msg_id = msg_id
        self.raw_email = raw_email
        self.msg = email.message_from_bytes(raw_email, policy=policy.default)
        self.base_path = base_path

        date = self.msg.get("Date", "no_date")
        if date != "no_date":
            try:
                date = email.utils.parsedate_to_datetime(date).strftime("%Y-%m-%d")
            except ValueError:
                from datetime import datetime
                try:
                    date = datetime.strptime(date, "%d-%m-%Y %H:%M:%S").strftime(
                        "%Y-%m-%d"
                    )
                except ValueError:
                    date = "no_date"

        folder_path = self.base_path / f"{date}-{self.msg_id}"
        folder_path.mkdir(parents=True, exist_ok=True)
        self.folder_path = folder_path

    async def save_email_file(self):
        email_file_path = self.folder_path / "email.eml"
        async with aiofiles.open(email_file_path, "wb") as f:
            await f.write(self.raw_email)

    async def save_attachments(self):
        tasks = []
        for part in self.msg.iter_attachments():
            attachment_filename = part.get_filename()
            if attachment_filename:
                attachment_path = self.folder_path / sanitize_filename(
                    attachment_filename
                )
                if attachment_path.exists():
                    continue

                payload = part.get_payload(decode=True)
                if payload:
                    tasks.append(
                        asyncio.create_task(
                            self._async_write_file(attachment_path, payload)
                        )
                    )
        if tasks:
            await asyncio.gather(*tasks)

    async def _async_write_file(self, file_path: Path, data: bytes):
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(data)

    async def save_markdown(self):
        metadata_fields = ("From", "To", "CC", "BCC", "Subject", "Date")
        md = "| Field | Value |\n| --- | --- |\n"
        for field in metadata_fields:
            value = decode_header_value(self.msg.get(field, ""))
            md += f"| {field} | {value} |\n"

        body = self.msg.get_body(preferencelist=("plain", "html"))
        if body:
            content = body.get_content()
            if body.get_content_type() == "text/html":
                converter = html2text.HTML2Text()
                converter.ignore_links = False
                body_md = converter.handle(content)
            else:
                body_md = content
        else:
            body_md = ""

        md += "\n" + body_md

        self.folder_path.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(self.folder_path / "email.md", "w") as f:
            await f.write(md)

    async def process_all(self):
        await asyncio.gather(
            self.save_email_file(), self.save_attachments(), self.save_markdown()
        )
