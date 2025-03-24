import asyncio
import logging
import os
import signal
import zipfile
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import click
import docling
from dotenv import load_dotenv
from tqdm import tqdm

from MailProcessor import MailProcessor

LOG_LEVELS = {
    0: logging.WARNING,
    1: logging.INFO,
    2: logging.DEBUG,
}

load_dotenv()

async def shutdown(signal, loop) -> None:
    logging.info("Received exit signal %s... Cancelling tasks", signal.name)
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()

def zip_all_subdirectories(source_dir: str, zip_name: str):
    source_path = Path(source_dir)
    files_to_zip = [
        file
        for subdir in source_path.iterdir()
        if subdir.is_dir()
        for file in subdir.rglob("*")
        if file.is_file()
    ]
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in tqdm(files_to_zip, desc="Zipping files"):
            zipf.write(file, file.relative_to(source_path))

def process_mailbox(mailbox: str, server:str, username: str, password: str, save_path: Path, search: str):
    async def run():
        processor = MailProcessor(server, username, password, save_path)
        await processor.connect()
        await processor.process_mailbox(mailbox, search)
        await processor.disconnect()

    asyncio.run(run())
    return mailbox

async def multiprocess_main(server: str, username: str, password: str, save_path: Path, search: str):
    processor = MailProcessor(server, username, password, save_path)
    await processor.connect()
    await processor.retrieve_mailboxes()
    mailbox_names = processor.mailboxes
    await processor.disconnect()

    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                process_mailbox,
                mailbox,
                server,
                username,
                password,
                save_path,
                search,
            )
            for mailbox in mailbox_names
        ]

        results = await asyncio.gather(*tasks)
        logging.info("Processed mailboxes: %s", results)

async def async_main(server: str, username: str, password: str, save_path: Path, search: str):
    logging.info("Starting async main with server: %s", server)
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    processor = MailProcessor(server, username, password, save_path)
    await processor.connect()

    try:
        await processor.retrieve_mailboxes()
        await processor.process_all_mailboxes(search)
    except asyncio.CancelledError:
        logging.info("Processing cancelled. Exiting gracefully")
    finally:
        await processor.disconnect()


@click.command()
@click.option(
    "--server",
    default=lambda: os.getenv("IMAP_SERVER"),
    help="The IMAP server host.",
)
@click.option(
    "--username",
    default=lambda: os.getenv("IMAP_USERNAME"),
    help="The username for the IMAP account.",
)
@click.option(
    "--password",
    default=lambda: os.getenv("IMAP_PASSWORD"),
    help="The password for the imap account.",
)
@click.option("--search", default="ALL", help="The search criteria for the emails.")
@click.option(
    "--save-path",
    default=lambda: os.getenv("IMAP_SAVE_PATH", "./email"),
    help="The base directory ro save emails and attachments.",
)
@click.option(
    "-v", "--verbose",
    count=True, 
    help="Increase the verbosity of the logging output. (-v for infor, -vv for debug)"
)
def main(server: str, username: str, password: str, search: str, save_path: str, verbose: int):
    logging_level = LOG_LEVELS.get(verbose, logging.WARNING)
    logging.basicConfig(
        level=logging_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        path = Path(save_path)
        asyncio.run(async_main(server, username, password, path, search))
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Exiting")

    zip_all_subdirectories(
        save_path, f"email_backup-{datetime.today().strftime('%Y-%M-%d')}.zip"
    )


if __name__ == "__main__":
    main()
