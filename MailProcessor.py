import asyncio
import asyncio.locks
import logging
import os
from pathlib import Path

import aioimaplib

from MailboxProcessor import MailboxProcessor


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

    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        search_criteria: str,
        base_path: Path,
    ):
        self.server = server
        self.username = username
        self.password = password
        self.search_criteria = search_criteria.split()
        self.batch_size = int(os.getenv("BATCH_SIZE", "50"))
        self.max_connections = int(os.getenv("MAX_CONNECTIONS", "10"))
        aioimaplib.IMAP4.timeout = 30

        base_path.mkdir(parents=True, exist_ok=True)
        self.base_path = base_path

        self.connection = None
        self.mailboxes = None
        self.connection_pool = []
        self.pool_lock = asyncio.Lock()

    async def connect(self):
        """Connect and login to mail server"""
        logging.info("Connecting to %s", self.server)
        try:
            self.connection = aioimaplib.IMAP4_SSL(self.server)
            await self.connection.wait_hello_from_server()

            response = await self.connection.login(self.username, self.password)
            if response.result != "OK":
                logging.error("Connection to %s Failed", self.server)
            else:
                logging.info("Connection to %s was Successful", self.server)
        except ConnectionError as e:
            logging.error("Failed to create connection to %s: %s", self.server, str(e))

    async def disconnect(self):
        """Logout from the mail server"""
        disconnect_tasks = []

        if self.connection:
            disconnect_tasks.append(
                asyncio.create_task(
                    self._safe_logout(self.connection, "main connection")
                )
            )
            self.connection = None

        async with self.pool_lock:
            for conn in self.connection_pool:
                disconnect_tasks.append(
                    asyncio.create_task(self._safe_logout(conn, "pooled connection"))
                )
            self.connection_pool = []

        if disconnect_tasks:
            await asyncio.wait(disconnect_tasks, timeout=15)

        logging.info("Closed all connections to IMAP Server %s", self.server)

    async def _safe_logout(self, connection, conn_type="connection"):
        try:
            await asyncio.wait_for(connection.logout(), timeout=10)
            logging.debug("Successfully closed %s to %s", conn_type, self.server)
        except Exception as e:
            logging.warning("Error closing %s to %s: %s", conn_type, self.server, str(e))

    async def get_connection(self):
        """
        Get a connection from the connection pool.
        This checks if the connection is stale. If the connection is stale,
        it will attempt to logout from the connection
        If there are no connections in the connection pool, then a new connection
        will be created.
        """
        max_retries = 3
        retry_delay = 2

        async with self.pool_lock:
            if self.connection_pool:
                connection = self.connection_pool.pop()
                try:
                    response = await asyncio.wait_for(connection.noop(), timeout=5)
                    if response.result == "OK":
                        return connection
                except Exception as e:
                    logging.warning(
                        "Pool connection is stale, creating new one: %s", str(e)
                    )
                    try:
                        await connection.logout()
                    except Exception:
                        pass

        for attempt in range(1, max_retries + 1):
            try:
                connection = aioimaplib.IMAP4_SSL(self.server)
                await asyncio.wait_for(connection.wait_hello_from_server(), timeout=10)
                response = await asyncio.wait_for(
                    connection.login(self.username, self.password), timeout=15
                )

                if response.result != "OK":
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        continue

                    raise ConnectionError(
                        f"Login to {self.server} failed after {max_retries} attempts"
                    )

                return connection
            except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                if attempt < max_retries:
                    logging.warning(
                        "Connection attempt %d failed: %s. Retrying in %ds",
                        attempt,
                        str(e),
                        retry_delay,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logging.error(
                        "Failed to connect to %s after %s attempts",
                        self.server,
                        max_retries,
                    )
                    raise
                raise

    async def release_connection(self, connection):
        if not connection:
            return

        try:
            response = await asyncio.wait_for(connection.noop(), timeout=5)
            if response.result == "OK":
                async with self.pool_lock:
                    if len(self.connection_pool) < self.max_connections:
                        self.connection_pool.append(connection)
                        return True

                    await asyncio.wait_for(connection.logout(), timeout=5)
                    return False
        except Exception as e:
            logging.warning("Connection is not reusable: %s", str(e))

        try:
            await asyncio.wait_for(connection.logout(), timeout=5)
        except Exception as e:
            logging.debug("Connection is not reusable: %s", str(e))

        return False

    async def retrieve_mailboxes(self):
        """ "Populate the list of all available mailboxes on the server"""
        logging.info("Getting mailbox names on %s", self.server)
        if not self.connection:
            logging.error("No connection to mail server %s", self.server)
            return

        status, mailboxes = await self.connection.list('""', "*")
        if status != "OK":
            logging.error("Cannot retrieve list of mailboxes")
            return

        self.mailboxes = list(
            filter(None, (extract_mailbox_name(mailbox) for mailbox in mailboxes))
        )
        logging.debug("Retrieved %d mailboxes: %s", len(self.mailboxes), self.mailboxes)

    async def process_all_mailboxes(self):
        if not self.mailboxes:
            logging.error("There are no mailboxes to process for %s", self.server)
            return

        semaphore = asyncio.Semaphore(self.max_connections)
        failed_mailboxes = []

        async def process_mailbox(mailbox_name):
            async with semaphore:
                connection = None

                try:
                    connection = await self.get_connection()

                    handler = MailboxProcessor(
                        connection, mailbox_name, self.search_criteria, self.base_path
                    )
                    await handler.process()

                    if not await self.release_connection(connection):
                        connection = None
                except asyncio.TimeoutError:
                    logging.error(
                        "Timeout processing mailbox %s - will retry later", mailbox_name
                    )
                    failed_mailboxes.append(mailbox_name)
                except Exception as e:
                    logging.error(
                        "Error processing mailbox %s: %s", mailbox_name, str(e)
                    )
                    failed_mailboxes.append(mailbox_name)

        mailbox_tasks = [
            process_mailbox(mailbox_name) for mailbox_name in self.mailboxes
        ]
        await asyncio.gather(*mailbox_tasks)

        return failed_mailboxes
