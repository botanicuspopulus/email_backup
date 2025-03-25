import asyncio
import itertools
import logging
import os
import time
from pathlib import Path


import aioimaplib
import aiofiles
from tqdm.asyncio import tqdm

from EmailHandler import EmailHandler, sanitize_filename


class MailboxProcessor:

    def __init__(
        self, connection, mailbox_name: str, search_criteria: list[str], base_path: Path
    ):
        self.connection = connection
        self.mailbox_name = mailbox_name
        self.search_criteria = search_criteria
        self.base_path = base_path
        self.batch_size = int(os.getenv("BATCH_SIZE", "50"))
        self.pbar = tqdm(desc=f"Processing Emails for {mailbox_name}", leave=False)
        self.tracking_file = self.base_path / f".tracking/{sanitize_filename(self.mailbox_name)}_processed_emails.txt"

    async def batch_fetch_emails(self):
        """Batch download of emails from the mailbox on the mailserver"""
        response = await self.connection.search(*self.search_criteria)
        if response.result != "OK":
            logging.error(
                "Batched email fetch search for %s failed: %s",
                " ".join(self.search_criteria),
                response.result,
            )
            return

        if len(response.lines) < 2:
            logging.info(
                "No search results for %s in %s",
                " ".join(self.search_criteria),
                self.mailbox_name,
            )
            return

        logging.info(
            "Fetching emails from %s with batch size %i",
            self.mailbox_name,
            self.batch_size,
        )
        message_ids = set(id.decode('utf-8') for id in response.lines[0].split())

        processed_ids = set()
        try:
            async with aiofiles.open(self.tracking_file, "r") as f:
                async for line in f:
                    processed_ids.add(line.strip())
        except OSError:
            logging.warning("The tracking file %s does not exist", self.tracking_file)

        unprocessed_ids = list(message_ids - processed_ids)

        self.pbar.reset(len(unprocessed_ids))
        for batch in itertools.batched(unprocessed_ids, self.batch_size):
            id_string = ",".join(batch)

            logging.debug("Fetching the following email IDs: %s", id_string)
            try:
                response = await self.connection.fetch(id_string, "(BODY[])")
            except aioimaplib.aioimaplib.CommandTimeout:
                logging.error(
                    "Failed to fetch batch due to timeout for message ids: %s",
                    id_string,
                )
                continue
            finally:
                if response.result != "OK":
                    logging.error("Fetch failed for batch with ids: %s", id_string)
                    continue

                for items in itertools.batched(response.lines[:-2], n=3):
                    msg_id = items[0].decode().split()[0]
                    raw_email = items[1]
                    yield msg_id, raw_email

    async def process(self):
        """
        Process the emails within the mailbox that match the search criteria.
        The download of the emails to process is handled in a batched manner
        """
        response = await self.connection.select(f'"{self.mailbox_name}"')
        if response.result != "OK":
            logging.error(
                "Cannot select mailbox %s: %s", self.mailbox_name, response.result
            )
            return

        max_wait_seconds = 30

        email_tasks = set()
        batch_size = self.batch_size
        last_process_time = time.time()

        async for msg_id, email in self.batch_fetch_emails():
            handler = EmailHandler(msg_id, self.mailbox_name, email, self.base_path)
            task = asyncio.create_task(handler.save_all())
            task.add_done_callback(email_tasks.discard)
            email_tasks.add(task)

            current_time = time.time()
            if len(email_tasks) >= batch_size or (
                current_time - last_process_time >= max_wait_seconds and email_tasks
            ):
                async for done in asyncio.as_completed(email_tasks):
                    await done
                    self.pbar.update()

                email_tasks = set()
                last_process_time = current_time

        async for done in asyncio.as_completed(email_tasks):
            await done
            self.pbar.update()
