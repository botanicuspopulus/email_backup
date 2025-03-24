import asyncio
import itertools
import logging
import os
import re
import time
from pathlib import Path

import aioimaplib
from aioimaplib import Response
from tqdm import tqdm

from EmailHandler import EmailHandler


def extract_mailbox_name(line: bytes) -> str | None:
    # Convert the byte string to a normal string.
    decoded = line.decode()
    if decoded.lower().startswith("list completed"):
        return None

    # If the mailbox name is quoted, extract the text within quotes.
    if '"' in decoded:
        parts = decoded.rsplit('"', 2)
        if len(parts) >= 3:
            return parts[-2]

    # Fallback: split by space and take the last part.
    return decoded.rsplit(" ", 1)[-1].strip('"')


class MailProcessor:
    def __init__(self, server: str, username: str, password: str, base_path: Path):
        self.server = server
        self.username = username
        self.password = password
        self.base_path = base_path
        self.connection = None
        self.mailboxes = None
        self.batch_size = int(os.environ.get("BATCH_SIZE", 50))

    async def connect(self):
        self.connection = aioimaplib.IMAP4_SSL(self.server)
        await self.connection.wait_hello_from_server()
        response = await self.connection.login(self.username, self.password)

        if response.result != "OK":
            logging.error("Login Failed")
        else:
            logging.info("Connection Successful")

    async def disconnect(self):
        if self.connection:
            await self.connection.logout()
            logging.info("Logged out from IMAP Server")

    async def retrieve_mailboxes(self):
        if not self.connection:
            logging.error("No connection to mail server")
            return

        status, mailboxes = await self.connection.list('""', "*")
        if status != "OK":
            logging.error("Cannot retrieve list of mailboxes")
            return

        self.mailboxes = list(
            filter(None, (extract_mailbox_name(mailbox) for mailbox in mailboxes))
        )

    async def process_all_mailboxes(self, search_criteria: str = "ALL"):
        if not self.mailboxes:
            logging.error("There are no mailboxes to process")
            return

        mailbox_tasks = []
        for mailbox in self.mailboxes:
            mailbox_tasks.append(
                asyncio.create_task(self.process_mailbox(mailbox, search_criteria))
            )

        if mailbox_tasks:
            pbar = tqdm(total=len(mailbox_tasks), desc="Processing all mailboxes")
            for task in asyncio.as_completed(mailbox_tasks):
                try:
                    await task
                except asyncio.CancelledError:
                    logging.info("Mailbox processing task cancelled")
                    raise

                pbar.update(1)
            pbar.close()

    async def batch_fetch_emails(
        self, connection, search_criteria: str = "ALL"):
        criteria_args = search_criteria.split()
        response = await connection.search(*criteria_args)
        if response.result != "OK":
            logging.error("Search of batched email fetch failed: %s", response.result)
            return

        if len(response.lines) < 2:
            logging.info("No search results for current mailbox")
            return

        message_numbers = response.lines[0]

        msg_ids = message_numbers.split()
        batch_size = self.batch_size
        for i in tqdm(
            range(0, len(msg_ids), batch_size), desc="Fetching batch of emails"
        ):
            batch = msg_ids[i : i + batch_size]
            id_string = ",".join(mid.decode("utf-8") for mid in batch)
            response = await connection.fetch(id_string, "(BODY[])")
            if response.result != "OK":
                logging.error("Fetch failed for batch %s", id_string)
                continue

            for items in itertools.batched(response.lines[:-2], n=3):
                mid = items[0].decode().split()[0]
                raw_email = items[1]
                yield mid, raw_email

    async def _process_email_tasks(self, tasks, mailbox_name):
        if not tasks:
            return

        pbar = tqdm(total=len(tasks), desc=f"Processing emails in {mailbox_name}")
        for task in asyncio.as_completed(tasks):
            try:
                await task
            except asyncio.CancelledError:
                logging.info("Email processing task cancelled")
                raise

            pbar.update(1)
        pbar.close()

    async def process_mailbox(
        self, mailbox_name: str = "INBOX", search_criteria: str = "ALL"
    ):
        connection = aioimaplib.IMAP4_SSL(self.server)
        await connection.wait_hello_from_server()

        response = await connection.login(self.username, self.password)
        if response.result != "OK":
            logging.error("Login failed for %s", mailbox_name)
            return
        logging.info("Successfully connected to %s for %s", self.server, mailbox_name)

        response = await connection.select(f'"{mailbox_name}"')
        if response.result != "OK":
            logging.error("Cannot select mailbox folder: %s", mailbox_name)
            await connection.logout()
            return
        logging.info("Successfully selected %s", mailbox_name)

        max_wait_seconds = 5

        tasks = []
        batch_size = self.batch_size
        last_proces_time = time.time()
        async for mid, email_item in self.batch_fetch_emails(
            connection, search_criteria
        ):
            handler = EmailHandler(mid, email_item, Path(self.base_path) / mailbox_name)
            tasks.append(asyncio.create_task(handler.process_all()))

            current_time = time.time()
            if len(tasks) >= batch_size or (
                current_time - last_proces_time >= max_wait_seconds and tasks
            ):
                await self._process_email_tasks(tasks, mailbox_name)
                tasks = []
                last_proces_time = current_time

        if tasks:
            await self._process_email_tasks(tasks, mailbox_name)

        await connection.logout()
