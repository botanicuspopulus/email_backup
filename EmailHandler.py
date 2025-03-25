import asyncio
import email
import itertools
import logging
import re
from datetime import datetime
from email import policy
from email.header import decode_header
from functools import lru_cache
from pathlib import Path

import aiofiles
import html2text


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

    def __init__(
        self, msg_id: str, mailbox_name: str, raw_email: bytes, base_path: Path
    ):
        self.msg_id = msg_id
        self.base_path = base_path
        self.mailbox_name = sanitize_filename(mailbox_name)

        self.raw_email = raw_email
        self.msg = email.message_from_bytes(raw_email, policy=policy.default)

        date = self.msg.get("Date", "no_date")
        if date != "no_date":
            try:
                date = email.utils.parsedate_to_datetime(date).strftime("%Y-%m-%d")
            except ValueError:
                try:
                    date = datetime.strptime(date, "%d-%m-%Y %H:%M:%S").strftime(
                        "%Y-%m-%d"
                    )
                except ValueError:
                    date = "no_date"

        folder_path = base_path / self.mailbox_name / f"{date}-{msg_id}"
        folder_path.mkdir(parents=True, exist_ok=True)
        self.folder_path = folder_path

    async def mark_email_as_processed(self):
        """Marks the email that has been processed by recording it in the .tracking folder"""
        tracking_dir = self.base_path / ".tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)

        tracking_file = tracking_dir / f"{self.mailbox_name}_processed_emails.txt"
        async with aiofiles.open(tracking_file, "a") as f:
            await f.write(f"{self.msg_id}\n")

    async def save_email_file(self):
        """
        Save the raw email bytes as an .eml file if any additional information
        needs to be processed later
        """
        email_file_path = self.folder_path / "email.eml"

        if email_file_path.exists():
            logging.debug("File already exists: %s", email_file_path)
            return

        async with aiofiles.open(email_file_path, "wb") as f:
            await f.write(self.raw_email)

    async def save_attachments(self):
        """
        Iterates over the attachments of the email and saves them in the same directory
        as the email
        """

        async def write_file_chunked(filepath: Path, part):
            payload = part.get_payload(decode=True)
            if not payload:
                return True

            buffer_size = 32 * 1024

            try:
                async with aiofiles.open(filepath, "wb") as f:
                    for i in range(0, len(payload), buffer_size):
                        chunk = payload[i:i + buffer_size]
                        await f.write(chunk)

                return True
            except OSError as e:
                logging.error("Failed to save attachment %s: %s", filepath, str(e))
                return False

        attachment_tasks = set()
        for part in self.msg.iter_attachments():
            attachment_filename = part.get_filename()
            if not attachment_filename:
                continue

            attachment_path = self.folder_path / sanitize_filename(attachment_filename)
            if attachment_path.exists():
                continue

            if len(attachment_tasks) >= 5:
                done, attachment_tasks = await asyncio.wait(
                    attachment_tasks, return_when=asyncio.FIRST_COMPLETED
                )

            attachment_tasks.add(
                asyncio.create_task(write_file_chunked(attachment_path, part))
            )

        await asyncio.gather(*attachment_tasks, return_exceptions=True)

    async def save_markdown(self):
        """Saves the email in the form of a markdown and includes some of the header fields and the body of the email"""
        filepath = self.folder_path / "email.md"

        if filepath.exists():
            logging.debug("File already exists: %s", filepath)
            return

        converter = html2text.HTML2Text()
        converter.ignore_links = False
        converter.body_width = 0

        metadata_fields = ("From", "To", "CC", "BCC", "Subject", "Date")
        md = "| Field | Value |\n| --- | --- |\n"
        for field in metadata_fields:
            value = decode_header_value(self.msg.get(field, ""))
            md += f"| {field} | {value} |\n"

        body = self.msg.get_body(preferencelist=("plain", "html"))
        body_md = ""

        if body:
            content = body.get_content()
            if body.get_content_type() == "text/html":
                body_md = await asyncio.get_event_loop().run_in_executor(
                    None, converter.handle, content
                )
            else:
                body_md = content

        md += "\n" + body_md

        async with aiofiles.open(filepath, "w") as f:
            await f.write(md)

    async def save_all(self):
        tasks = [
            self.save_email_file(),
            self.save_markdown(),
            self.save_attachments() if any(self.msg.iter_attachments()) else None
        ]

        await asyncio.gather(*[t for t in tasks if t is not None])

        await self.mark_email_as_processed()
