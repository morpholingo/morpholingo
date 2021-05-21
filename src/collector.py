#!/usr/bin/env python3

import asyncio
import os.path
import sys
import urllib.parse
from typing import NamedTuple, Optional, Text

import aiofiles
import aiohttp
import requests
from rich.console import Console


class FWRes(NamedTuple):
    url: Text
    params: dict[str, str]
    filepath: Optional[Text]
    http_status: int
    write_success: bool


def _get_filepath(params_dict, dirpath):
    """Returns filepath from base file name in URL and directory path."""
    filename = f"{params_dict['pageid']}.json"
    return os.path.join(dirpath, filename)


async def async_fetch(session, url):
    """Asynchronous I/O HTTP GET request with a ClientSession instantiated
    from the aiohttp library."""
    api_url = url[0]
    params = url[1]
    async with session.get(api_url, params=params) as response:
        status = response.status
        if status != 200:
            text = None
        else:
            text = await response.text(encoding="utf-8")
        return api_url, params, status, text


async def async_write_text(path, text):
    """Asynchronous IO writes of text data `text` to disk on the file path `path`"""
    async with aiofiles.open(path, "w") as f:
        await f.write(text)


async def async_fetch_and_write(session, url, dirpath):
    """Asynchronous I/O HTTP GET request with a ClientSession instantiated
    from the aiohttp library, followed by an asynchronous I/O file write of
    the binary to disk with the aiofiles library.
    :returns tuple with url, filepath, http_status, write_success items"""
    url, params, status, text = await async_fetch(session, url)
    if status != 200:
        filepath = None
        write_success = False
    else:
        filepath = _get_filepath(params, dirpath)
        await async_write_text(filepath, text)
        write_success = True

    return FWRes(
        url=url,
        params=params,
        filepath=filepath,
        http_status=status,
        write_success=write_success,
    )


async def create_async_get_request_session_and_run(urls, dirpath):
    """Creates an aiohttp library ClientSession and performs asynchronous GET requests +
    binary file writes with the binary response from the GET request.
    :returns list of asyncio Tasks that include tuples of response data
    (defined in async_fetch_and_write)"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            # use asyncio.ensure_future instead of .run() here to maintain
            # Py3.6 compatibility
            task = asyncio.ensure_future(async_fetch_and_write(session, url, dirpath))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)
        return tasks


def async_fetch_files(dirpath, urls):
    """Main entry point for the asynchronous GET requests for text files"""
    loop = asyncio.get_event_loop()
    tasks = loop.run_until_complete(
        create_async_get_request_session_and_run(urls, dirpath)
    )
    for task in tasks:
        if task.exception():
            # raise exception here to notify calling code that something
            # did not work
            raise AIOError(f"{task.exception()}")
        elif task.result().http_status != 200:
            # handle non-200 HTTP response status codes + file write fails
            raise AIOError(
                f"failed to pull '{task.result().url}' with ID "
                f"{task.result().params['pageid']}: HTTP status "
                f"code {task.result().http_status}"
            )


def get_random_wikipedia_article_ids_by_lang(lang, request_number):
    wiki_api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params_randomizer = {
        "format": "json",
        "action": "query",
        "list": "random",
        "rnnamespace": "0",
        "prop": "revisions",
        "rvprop": "content",
        "rnlimit": f"{request_number}",
    }

    res = requests.get(wiki_api_url, params_randomizer)
    json_res_random = res.json()
    return json_res_random["query"]["random"]


def generate_content_url_list(lang, random_article_id_dict):
    url_list = []
    wiki_api_url = f"https://{lang}.wikipedia.org/w/api.php"

    for key, _ in random_article_id_dict.items():
        params_content = {
            "action": "parse",
            "format": "json",
            "curtimestamp": "1",
            "uselang": "content",
            "prop": "text",
            "formatversion": "2",
            "pageid": f"{key}",
        }
        url_list.append((wiki_api_url, params_content))

    return url_list


class AIOError(Exception):
    pass


def main(argv):
    lang = argv[0]
    article_number = argv[1]

    console = Console()

    console.log("Start Collection")
    with console.status(
        f"Pulling {article_number} files from `{lang}` Wikipedia...", spinner="dots10"
    ):
        random_list = get_random_wikipedia_article_ids_by_lang(lang, article_number)
        article_dict = {}
        for item in random_list:
            article_dict[item["id"]] = {"title": item["title"]}

        url_list = generate_content_url_list(lang, article_dict)

        async_fetch_files("tmp", url_list)

    console.log("End Collection")


if __name__ == "__main__":
    main(sys.argv[1:])
