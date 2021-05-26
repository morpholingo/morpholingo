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
from parsers.html import parse_html_p_content
from preprocessing.cleaners import clean_wikipedia
from utils.strutils import utf8len

# tuple of localized `title` attribute defintions in Wikipedia stub
# article HTML `a href` tags
WIKI_STUB_TITLES = {
    "en": "Wikipedia:Stub",  # English
    "he": "ויקיפדיה:קצרמר",  # Hebrew
}


async def async_read(filepath):
    """Asynchronous IO text file reads"""
    async with aiofiles.open(filepath, "r") as f:
        return await f.read()


# ==================
# Exclusion criteria
# ==================


async def is_exclusion_stub(html_text, lang_tag):
    if lang_tag in WIKI_STUB_TITLES:
        soup = BeautifulSoup(html_text, "lxml")
        stub_title = WIKI_STUB_TITLES[lang_tag]
        # if we identify the stub HTML tag, it is a stub
        if soup.find("a", {"title": stub_title}):
            return True

    return False


def is_exclusion_empty_content(parsed_p_tag_content):
    return parsed_p_tag_content is None


def is_exclusion_content_size(parsed_p_tag_content):
    """Exclude articles below a content size threshold.
    Defined at the 25th percentile of English article sizes."""
    if utf8len(clean_wikipedia(parsed_p_tag_content)) < 275:
        return True
    else:
        return False


# ==================
# Async file I/O
# ==================


async def remove_file(filepath):
    """Asynchronous removal of file on path `filepath`"""
    os.remove(filepath)


async def remove_files(filepaths, lang_tag):
    """Asynchronous file removal based on inclusion/exclusion criteria."""
    remove_path_list = []
    stub_file_count = 0
    empty_content_count = 0
    below_content_size_count = 0

    for filepath in filepaths:
        json_text = await async_read(filepath)
        json_obj = json.loads(json_text)
        html_text = json_obj["parse"]["text"]
        # exclusion criterion: Is a stub file
        if await is_exclusion_stub(html_text, lang_tag):
            remove_path_list.append(filepath)
            stub_file_count += 1
        else:
            parsed_p_tag_content = parse_html_p_content(html_text)
            if is_exclusion_empty_content(parsed_p_tag_content):
                remove_path_list.append(filepath)
                empty_content_count += 1
            # the above check confirms that the parsed <p> tag content
            # response is not None.  Keep this check below (also because
            # this is likely slower)
            elif is_exclusion_content_size(parsed_p_tag_content):
                remove_path_list.append(filepath)
                below_content_size_count += 1

    # remove files that meet exclusion criteria
    for remove_filepath in remove_path_list:
        await remove_file(remove_filepath)

    # report number of articles that met exclusion criteria
    if stub_file_count > 0:
        print(f"Removed {stub_file_count} stub articles")
    if empty_content_count > 0:
        print(f"Removed {empty_content_count} empty content articles")
    if below_content_size_count > 0:
        print(
            f"Removed {below_content_size_count} articles below content "
            f"size criterion"
        )
    removed_file_count = (
        stub_file_count + empty_content_count + below_content_size_count
    )
    return removed_file_count


def cull(filepaths, lang_tag):
    """Asynchronous file removal based on inclusion/exclusion criteria"""
    loop = asyncio.get_event_loop()
    removed_file_count = loop.run_until_complete(remove_files(filepaths, lang_tag))
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
    parser.add_argument("LANGTAG", help="Language tag")
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
        removed_file_count = cull(filepaths, args.LANGTAG)

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
