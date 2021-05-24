#!/usr/bin/env python3

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import aiofiles
from bs4 import BeautifulSoup
from rich.console import Console

from meta import __version__


async def async_read(filepath):
    """Asynchronous IO text file reads"""
    async with aiofiles.open(filepath, "r") as f:
        return await f.read()


async def parse_content(json_text):
    """Asynchronous content text parsing"""
    json_obj = json.loads(json_text)
    html_text = json_obj["parse"]["text"]
    soup = BeautifulSoup(html_text, "lxml")
    contents = soup.find_all("p")
    content_list = []
    for content in contents:
        content_list.append(content.text)

    return "\n".join(content_list)


# ==================
# Exclusion criteria
# ==================


async def is_exclusion_stub(text):
    return "is a stub" in text


# ==================
# Async file I/O
# ==================


async def remove_file(filepath):
    """Asynchronous removal of file on path `filepath`"""
    os.remove(filepath)


async def remove_files(filepaths):
    """Asynchronous file removal based on inclusion/exclusion criteria."""
    remove_path_list = []
    stub_file_count = 0

    for filepath in filepaths:
        json_text = await async_read(filepath)
        content_text = await parse_content(json_text)
        # exclusion criterion: Is a stub file
        if await is_exclusion_stub(content_text):
            remove_path_list.append(filepath)
            stub_file_count += 1

    # remove files that meet exclusion criteria
    for remove_filepath in remove_path_list:
        await remove_file(remove_filepath)

    removed_file_count = stub_file_count
    return removed_file_count


def cull(filepaths):
    """Asynchronous file removal based on inclusion/exclusion criteria"""
    loop = asyncio.get_event_loop()
    removed_file_count = loop.run_until_complete(remove_files(filepaths))
    return removed_file_count


def main(argv):
    # ------------------------------------------
    # argparse command line argument definitions
    # ------------------------------------------
    parser = argparse.ArgumentParser(
        description="JSON data file cull based on inclusion/exclusion criteria"
    )
    parser.add_argument(
        "--version", action="version", version=f"culler.py v{__version__}"
    )
    parser.add_argument("TARGETDIR", help="Write directory path")
    args = parser.parse_args(argv)

    console = Console()

    console.log("Start culling")
    console.print(
        f"Culling directory [blue]{args.TARGETDIR}[/blue] according to inclusion/"
        f"exclusion criteria."
    )

    with console.status(
        "Processing...",
        spinner="dots10",
    ):
        # Defined as JSON filepaths in the directory that was requested by user
        filepaths = list(Path(args.TARGETDIR).glob("*.json"))
        start_filepath_count = len(filepaths)
        console.print(f"Start file count: {start_filepath_count}")

        # cull files according to inclusion/exclusion criteria
        removed_file_count = cull(filepaths)

        # file removal report
        remaining_files = len(list(Path(args.TARGETDIR).glob("*.json")))
        console.print(f"Remaining file count: {remaining_files}")
        if removed_file_count > 0:
            console.print(
                f"Removed {removed_file_count} files that met exclusion criteria"
            )
        else:
            console.print("No files were removed.")

    console.log("End culling")


if __name__ == "__main__":
    main(sys.argv[1:])
