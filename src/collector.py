#!/usr/bin/env python3

import argparse
import asyncio
import math
import os
import os.path
import sys
from typing import NamedTuple, Optional, Text

import aiofiles
import aiohttp
import requests
from rich.console import Console

from meta import __version__


class MLRes(NamedTuple):
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

    return MLRes(
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
    if res.status_code == 200:
        json_res_random = res.json()
        return json_res_random["query"]["random"]
    else:
        sys.stderr.write(f"Failed with HTTP status code: {res.status_code}\n")
        sys.exit(1)


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


def chunk(total, chunksize):
    # if the chunk size exceeds total request
    # return the total request size as this
    # is an appropriate chunk
    if chunksize > total:
        return [total]

    # otherwise establish number of chunksize
    # chunks for requests
    factor = math.floor(total / chunksize)
    modulo = total % chunksize
    product = factor * chunksize
    # add `factor` number of chunks
    chunk_list = [f"{chunksize}"] * factor
    if modulo > 0:
        chunk_list.append(f"{total - product}")

    return chunk_list


class AIOError(Exception):
    pass


def main(argv):

    # ------------------------------------------
    # argparse command line argument definitions
    # ------------------------------------------
    parser = argparse.ArgumentParser(description="Text data collector")
    parser.add_argument(
        "--version", action="version", version=f"collector.py v{__version__}"
    )
    parser.add_argument("LANG", help="Wikipedia language tag")
    parser.add_argument("NUMBER", help="Number of articles")
    parser.add_argument("TARGETDIR", help="Write directory path")
    args = parser.parse_args(argv)

    console = Console()

    console.log("Start Collection")
    with console.status(
        f"Pulling articles from `{args.LANG}` Wikipedia...",
        spinner="dots10",
    ):
        try:
            random_list = []
            # maximum number of random article requests defined by
            # the wikipedia API
            wikipedia_max = 500
            # chunk requests @ maximum allowed random article requests
            # by the wikipedia API
            chunked_article_numbers = chunk(int(args.NUMBER), wikipedia_max)
            if len(chunked_article_numbers) > 1:
                console.print(
                    f"Chunking random article requests @ API max: "
                    f"{chunked_article_numbers}"
                )
            for chunk_number in chunked_article_numbers:
                random_list.extend(
                    get_random_wikipedia_article_ids_by_lang(args.LANG, chunk_number)
                )

            # collect random articles with unique API id's into a dictionary
            article_dict = {}
            for item in random_list:
                article_dict[item["id"]] = {"title": item["title"]}

            # prep GET request URLs (including API params)
            url_list = generate_content_url_list(args.LANG, article_dict)

            # go get them with async GET requests + async writes
            async_fetch_files(args.TARGETDIR, url_list)
        except Exception as e:
            sys.stderr.write(
                f"Failed attempt to pull randomized data with error: {e}\n"
            )
            sys.exit(1)

    console.print(
        f"Pulled articles from `{args.LANG}` Wikipedia to directory path ==> "
        f"[blue underline]{args.TARGETDIR}[/blue underline]"
    )
    json_file_count = len(
        [name for name in os.listdir(args.TARGETDIR) if name.endswith(".json")]
    )
    console.print(
        f"Total JSON files in [blue underline]{args.TARGETDIR}[/blue underline] ==> "
        f"{json_file_count}"
    )
    console.log("End Collection")


if __name__ == "__main__":
    main(sys.argv[1:])
