import typing
import logging
from collections import defaultdict
import ujson
import os
import random
import re
import hashlib

import aiofiles
from httpx import Request, Response

from .items import Item, set_value, get_value
from .exceptions import Dropped
from .states import StateStorage, request_fingerprint
from .utils import run_cor_func


class Pipeline:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_spider_open(self) -> typing.Optional[typing.Awaitable]:
        """Call when ant open, this method can be coroutine function"""

    def on_spider_close(self) -> typing.Optional[typing.Awaitable]:
        """Call when ant close, this method can be coroutine function"""

    def process(self, obj: typing.Any) -> typing.Any:
        """Process objs, this method can be coroutine function
        Raise Dropped when drop one obj

        :raise Dropped
        """
        return obj


# Response pipelines
class ResponseFilterErrorPipeline(Pipeline):
    def process(self, obj: Response) -> Response:
        if obj.status_code >= 400:
            raise Dropped("Response - {:s}".format(str(obj)))
        else:
            return obj


# Request pipelines
class RequestDuplicateFilterPipeline(Pipeline):
    def __init__(self):
        self.__request_urls = set()
        super().__init__()

    def process(self, obj: Request) -> Request:
        if obj.url in self.__request_urls:
            raise Dropped("Request duplicate!")
        else:
            self.__request_urls.add(obj.url)
            return obj


class PersistentRequestDuplicateFilterPipeline(Pipeline):
    """Request duplicate filter backed by a persistent :class:`StateStorage`.

    A request fingerprint is the sha256 hash of the upper case HTTP method and
    the canonical URL (query parameters are sorted, repeated keys are kept;
    body and headers are ignored). Only requests whose response pipelines
    finish successfully are persisted as done, so a crash before that point
    makes the request eligible again on the next resume.

    The bound storage is created by :class:`~ant_nest.ant.Ant` according to
    settings; using this pipeline without a storage raises on spider open.
    """

    def __init__(self):
        super().__init__()
        self.storage: typing.Optional[StateStorage] = None

    def bind_storage(self, storage: StateStorage):
        self.storage = storage

    def on_spider_open(self):
        if self.storage is None:
            raise RuntimeError(
                "PersistentRequestDuplicateFilterPipeline requires a "
                "persistent StateStorage; enable PERSISTENT_STATE in settings"
            )

    def process(self, obj: Request) -> Request:
        assert self.storage is not None
        fingerprint = request_fingerprint(obj)
        if self.storage.is_request_known(fingerprint):
            raise Dropped("Request duplicate!")
        self.storage.add_pending_request(fingerprint)
        return obj


class RequestUserAgentPipeline(Pipeline):
    user_agent = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
    )

    def __init__(self, user_agent: str = user_agent):
        super().__init__()
        self.user_agent = user_agent

    def process(self, obj: Request) -> Request:
        obj.headers["user-agent"] = self.user_agent
        return obj


class RequestRandomUserAgentPipeline(Pipeline):
    """Create simple and common user agent for request by random,
    It`s easy to add new rule.
    """

    USER_AGENT_FORMAT = "Mozilla/5.0 ({system}) {browser}"
    SYSTEM_FORMATS = {
        "UnixLike": "X11; {unix-like_os} {cpu_type}",
        "MacOS": "Macintosh; Intel Mac OS X {macos_version}",
        "Windows": "Windows NT {windows_version}",
        "Android": "Android {android_version}; Linux",
        "iOS": "{ios_driver}; CPU OS {ios_version} like Mac OS X",
    }
    BROWSER_FORMATS = {
        "Firefox": "Gecko/20100101 Firefox/{firefox_version}",
        "Safari": "AppleWebKit/{webkit_version} (KHTML, like Gecko) "
        "Version/{safari_version} Safari/{safari_version2}",
        "Chrome": "AppleWebKit/{webkit_version} (KHTML, like Gecko) "
        "Chrome/{chrome_version} Safari/{safari_version2}",
    }
    FORMAT_VARS: typing.Dict[str, typing.Tuple[str, ...]] = {
        "unix-like_os": ("Linux", "FreeBSD"),
        "cpu_type": ("x86_64", "i386", "amd64"),
        "macos_version": (
            "10_10_1",
            "10_11_1",
            "10_11_2",
            "10_12_2",
            "10_12_3",
            "10_13_3",
        ),
        "windows_version": ("5_0", "5_1", "5_2", "6_0", "6_1", "10_0"),
        "android_version": ("4_4", "5_0", "5_1", "6_0", "6_1", "7_0", "7_1", "8_0"),
        "ios_driver": ("iPone", "iPod", "iPad"),
        "ios_version": ("6_0", "7_0", "8_1", "9_2", "10_3", "10_2_3", "11_3_3"),
        "firefox_version": ("27.3", "28.0", "31.0", "40.1"),
        "webkit_version": ("533.18.1", "533.19.4", "533.20.25", "534.55.3"),
        "safari_version": ("4.0.1", "5.0.4", "5.1.3", "6.0", "7.0.3"),
        "safari_version2": ("530.19.1", "531.9", "533.16", "533.20.27"),
        "chrome_version": (
            "41.0.2226.0",
            "60.0.1325.223",
            "62.0.1532.123",
            "64.0.3282.119",
        ),
    }

    def __init__(self, system: str = "random", browser: str = "random"):
        if system != "random" and system not in self.SYSTEM_FORMATS.keys():
            raise ValueError("The system {:s} is not supported!".format(system))
        if browser != "random" and browser not in self.BROWSER_FORMATS.keys():
            raise ValueError("The browser {:s} is not supported!".format(browser))

        self.system = system
        self.browser = browser
        super().__init__()

    @staticmethod
    def choice(data: typing.Sequence[str]) -> str:
        return random.choice(data)

    def _format(self, pattern: str) -> str:
        """format system or browser pattern string"""
        kv = {}
        keys = re.findall(r"{(\S+?)}", pattern)

        for key in keys:
            kv[key] = self.choice(self.FORMAT_VARS[key])

        return pattern.format(**kv)

    def create(self) -> str:
        if self.system != "random":
            system_format = self.SYSTEM_FORMATS[self.system]
        else:
            system_format = self.SYSTEM_FORMATS[
                self.choice(list(self.SYSTEM_FORMATS.keys()))
            ]

        if self.browser != "random":
            browser_format = self.BROWSER_FORMATS[self.browser]
        else:
            browser_format = self.BROWSER_FORMATS[
                self.choice(list(self.BROWSER_FORMATS.keys()))
            ]

        return self.USER_AGENT_FORMAT.format(
            system=self._format(system_format), browser=self._format(browser_format)
        )

    def process(self, obj: Request) -> Request:
        obj.headers["user-agent"] = self.create()
        return obj


class RequestRandomComputerUserAgentPipeline(Pipeline):
    SYSTEM_FORMATS = {
        "UnixLike": "X11; {unix-like_os} {cpu_type}",
        "MacOS": "Macintosh; Intel Mac OS X {macos_version}",
        "Windows": "Windows NT {windows_version}",
    }


class RequestRandomMobileUserAgentPipeline(Pipeline):
    SYSTEM_FORMATS = {
        "Android": "Android {android_version}; Linux",
        "iOS": "{ios_driver}; CPU OS {ios_version} like Mac OS X",
    }


# Item pipelines
class ItemPrintPipeline(Pipeline):
    def process(self, obj: Item) -> Item:
        self.logger.info(obj.__repr__())
        return obj


class ItemFieldReplacePipeline(Pipeline):
    """Replace some chars in item`s field string"""

    def __init__(
        self,
        fields: typing.Sequence[str],
        excess_chars: typing.Tuple[str, ...] = ("\r", "\n", "\t"),
    ):
        self.fields = fields
        self.excess_chars = excess_chars
        super().__init__()

    def process(self, obj: Item) -> Item:
        for field in self.fields:
            value: str = get_value(obj, field)
            for char in self.excess_chars:
                value = value.replace(char, "")
            set_value(obj, field, value)
        return obj


class ItemBaseFileDumpPipeline(Pipeline):
    @classmethod
    async def dump(
        cls, file_path: str, data: typing.Any, buffer_size: int = 1024 * 1024
    ):
        """Dump data(binary or text, stream or normal, async or not) to disk file.
        typing.IO data will be closed.
        """
        chunk = None
        if isinstance(data, str):
            file_mode = "w"
        elif isinstance(data, bytes):
            file_mode = "wb"
        elif hasattr(data, "read"):  # readable
            chunk = await run_cor_func(data.read, buffer_size)

            if isinstance(chunk, str):
                file_mode = "w"
            else:
                file_mode = "wb"
        else:
            raise ValueError(
                "The type {:s} is not supported".format(type(data).__class__.__name__)
            )

        async with aiofiles.open(file_path, file_mode) as file:  # type: ignore
            if chunk is not None:  # in streaming
                await file.write(chunk)
                while True:
                    chunk = await run_cor_func(data.read, buffer_size)

                    if len(chunk) == 0:
                        break
                    else:
                        await file.write(chunk)

                await run_cor_func(data.close)
            else:
                await file.write(data)


class ItemJsonDumpPipeline(ItemBaseFileDumpPipeline):
    """Dump item to json during pipeline closing"""

    def __init__(
        self, *, to_dict: typing.Callable[[Item], typing.Dict], file_dir: str = "."
    ):
        super().__init__()
        self.file_dir = file_dir
        self.data: typing.DefaultDict[str, typing.List[typing.Dict]] = defaultdict(list)
        self.to_dict = to_dict

    def process(self, obj: Item) -> Item:
        self.data[obj.__class__.__name__].append(self.to_dict(obj))
        return obj

    async def on_spider_close(self):
        for file_name, data in self.data.items():
            data = ujson.dumps(data)
            await self.dump(os.path.join(self.file_dir, file_name + ".json"), data)


class PersistentItemJsonDumpPipeline(ItemBaseFileDumpPipeline):
    """Append each item to a durable json-lines file while crawling.

    Unlike :class:`ItemJsonDumpPipeline`, items are written and fsync-ed in
    :meth:`process` instead of being buffered until the spider closes, so
    every committed item survives a process kill. Each output line is a json
    object with an ``id`` (the idempotency key) and a ``data`` payload built
    by ``to_dict``, keeping the same ``to_dict``/``file_dir`` parameters as
    :class:`ItemJsonDumpPipeline`.

    On open, the previous output file is repaired line by line: complete
    lines are re-registered as committed item keys and any torn final line is
    truncated, which makes the file always readable after a crash and keeps a
    logical item committed at most once across runs.
    """

    def __init__(
        self,
        *,
        to_dict: typing.Callable[[Item], typing.Dict],
        file_dir: str = ".",
        idempotency_key: typing.Optional[typing.Callable[[Item], str]] = None,
    ):
        super().__init__()
        self.file_dir = file_dir
        self.to_dict = to_dict
        self.idempotency_key = idempotency_key or self._default_idempotency_key
        self.storage: typing.Optional[StateStorage] = None
        self._files: typing.Dict[str, typing.Any] = {}

    def bind_storage(self, storage: StateStorage):
        self.storage = storage

    @staticmethod
    def _default_idempotency_key(item: Item) -> str:
        """Key derived from the canonical json representation of the item.

        Two equal items therefore share a key even across processes; supply a
        custom callable when the item carries a natural unique identifier.
        """
        payload = item if isinstance(item, typing.Dict) else item.__dict__
        raw = ujson.dumps(payload, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _file_path(self, namespace: str) -> str:
        return os.path.join(self.file_dir, namespace + ".jsonl")

    async def on_spider_open(self):
        if self.storage is None:
            raise RuntimeError(
                "PersistentItemJsonDumpPipeline requires a persistent "
                "StateStorage; enable PERSISTENT_STATE in settings"
            )
        os.makedirs(self.file_dir, exist_ok=True)

    @staticmethod
    async def _repair_output_file(file_path: str) -> typing.List[str]:
        """Drop a torn final line and return ids of all intact lines.

        Each line is decoded independently; everything from the first bad
        line (typically the half written last line after a kill) is removed
        by truncating the file at the end of the last good line.
        """
        if not os.path.exists(file_path):
            return []
        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            content = await file.read()

        good_ids: typing.List[str] = []
        good_offset = 0
        offset = 0
        for line in content.splitlines(keepends=True):
            try:
                record = ujson.loads(line)
                good_ids.append(record["id"])
            except (ValueError, KeyError, TypeError):
                break
            offset += len(line.encode("utf-8"))
            good_offset = offset

        if good_offset < len(content.encode("utf-8")):
            async with aiofiles.open(file_path, "r+b") as file:
                await file.truncate(good_offset)
                await file.flush()
                os.fsync(file.fileno())
        return good_ids

    async def _ensure_output(self, namespace: str):
        assert self.storage is not None
        if namespace in self._files:
            return self._files[namespace]

        file_path = self._file_path(namespace)
        recovered_ids = await self._repair_output_file(file_path)
        await self.storage.recover_items(namespace, recovered_ids)
        absolute_path = await self.storage.register_output(file_path, namespace)
        self._files[namespace] = await aiofiles.open(
            absolute_path, "a", encoding="utf-8"
        )
        return self._files[namespace]

    async def process(self, obj: Item) -> Item:
        assert self.storage is not None
        namespace = obj.__class__.__name__
        key = self.idempotency_key(obj)
        if self.storage.is_item_committed(key, namespace):
            raise Dropped("Item duplicate!")

        record = ujson.dumps({"id": key, "data": self.to_dict(obj)})
        file = await self._ensure_output(namespace)
        # Append the whole line first, then persist the idempotency key:
        # a crash between the two steps only leaves a line whose key is
        # replayed and deduplicated on the next open, never a lost item.
        await file.write(record + "\n")
        await file.flush()
        os.fsync(file.fileno())
        await self.storage.commit_item(key, namespace)
        return obj

    async def on_spider_close(self):
        for namespace, file in list(self._files.items()):
            await file.close()
        self._files.clear()


__all__ = [var for var in vars().keys() if "Pipeline" in var]
