import asyncio
import os
import shutil

import httpx
import pytest
import ujson

import aiofiles

from ant_nest import pipelines as pls
from ant_nest.ant import Ant
from ant_nest.exceptions import Dropped
from ant_nest.states import (
    Mode,
    StateCorruptedError,
    StateStorage,
    request_fingerprint,
)


@pytest.fixture()
def state_dir(tmp_path):
    path = str(tmp_path / "state")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture()
def item_cls():
    class Item:
        pass

    return Item


@pytest.mark.asyncio
async def test_request_fingerprint_is_stable_and_distinct():
    first = httpx.Request("get", "http://test.com/a?b=2&a=1")
    second = httpx.Request("GET", "http://test.com/a", params=[("a", "1"), ("b", "2")])
    post = httpx.Request("post", "http://test.com/a?b=2&a=1")
    assert request_fingerprint(first) == request_fingerprint(second)
    assert request_fingerprint(first) != request_fingerprint(post)


@pytest.mark.asyncio
async def test_request_pipeline_resume_skips_done_requests(state_dir):
    pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline.bind_storage(storage)
    pipeline.on_spider_open()

    request = httpx.Request("GET", "http://test.com/a")
    fingerprint = request_fingerprint(request)
    assert pipeline.process(request) is request
    assert storage.is_request_known(fingerprint)
    # pending only: a new process would still retry it
    await storage.close()

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    assert not storage.is_request_known(fingerprint)
    pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    pipeline.bind_storage(storage)
    await storage.mark_request_done(fingerprint)
    with pytest.raises(Dropped):
        pipeline.process(httpx.Request("GET", "http://test.com/a"))
    await storage.close()


@pytest.mark.asyncio
async def test_pipeline_requires_storage():
    request_pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    with pytest.raises(RuntimeError):
        request_pipeline.on_spider_open()
    item_pipeline = pls.PersistentItemJsonDumpPipeline(to_dict=lambda x: x.__dict__)
    with pytest.raises(RuntimeError):
        await item_pipeline.on_spider_open()


@pytest.mark.asyncio
async def test_item_pipeline_durable_append_and_idempotency(state_dir, item_cls):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()

    item = item_cls()
    item.title = "hello"
    assert await pipeline.process(item) is item
    key = pipeline.idempotency_key(item)
    assert storage.is_item_committed(key)
    with pytest.raises(Dropped):
        await pipeline.process(item)

    output_path = pipeline._file_path("Item")
    async with aiofiles.open(output_path) as file:
        lines = await file.readlines()
    assert len(lines) == 1
    assert ujson.loads(lines[0])["data"]["title"] == "hello"
    await pipeline.on_spider_close()
    await storage.close()


@pytest.mark.asyncio
async def test_torn_jsonl_line_is_repaired_on_resume(state_dir, item_cls):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()

    first = item_cls()
    first.title = "one"
    second = item_cls()
    second.title = "two"
    await pipeline.process(first)
    await pipeline.process(second)
    await pipeline.on_spider_close()
    await storage.checkpoint(force=True)
    await storage.close()

    output_path = os.path.join(state_dir, "items", "Item.jsonl")
    # simulate a kill while a third line is being written
    with open(output_path, "a", encoding="utf-8") as file:
        file.write('{"id": "half-written"')
        file.flush()
        os.fsync(file.fileno())

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()
    await pipeline._ensure_output("Item")
    with open(output_path, encoding="utf-8") as file:
        lines = [line for line in file.read().splitlines() if line]
    assert len(lines) == 2
    assert storage.committed_item_count == 2
    # replaying the second committed item is dropped, not duplicated
    with pytest.raises(Dropped):
        await pipeline.process(second)
    third = item_cls()
    third.title = "three"
    await pipeline.process(third)
    await pipeline.on_spider_close()
    await storage.close()


@pytest.mark.asyncio
async def test_torn_line_item_can_be_recollected(state_dir, item_cls):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()
    item = item_cls()
    item.key = "replay-me"
    await pipeline.process(item)
    await pipeline.on_spider_close()
    await storage.close()

    output_path = os.path.join(state_dir, "items", "Item.jsonl")
    # corrupt the only committed line while its key is still in the database
    with open(output_path, "w", encoding="utf-8") as file:
        file.write("torn line without newline")
        file.flush()
        os.fsync(file.fileno())

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()
    await pipeline._ensure_output("Item")
    assert not storage.is_item_committed("replay-me", "Item")
    # same logical item is accepted again, exactly one line on disk
    await pipeline.process(item)
    await pipeline.on_spider_close()
    with open(output_path, encoding="utf-8") as file:
        assert len(file.read().splitlines()) == 1
    assert storage.is_item_committed("replay-me", "Item")
    await storage.close()


@pytest.mark.asyncio
async def test_concurrent_requests_and_items(state_dir, item_cls):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    request_pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    request_pipeline.bind_storage(storage)
    item_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    item_pipeline.bind_storage(storage)
    await item_pipeline.on_spider_open()

    async def handle(index):
        request = httpx.Request("GET", "http://test.com/p/{:d}".format(index))
        try:
            request_pipeline.process(request)
        except Dropped:
            return
        await storage.mark_request_done(request_fingerprint(request))
        item = item_cls()
        item.key = "item-{:d}".format(index)
        try:
            await item_pipeline.process(item)
        except Dropped:
            return

    await asyncio.gather(*(handle(index % 50) for index in range(200)))
    assert storage.processed_request_count == 50
    assert storage.committed_item_count == 50
    with open(os.path.join(state_dir, "items", "Item.jsonl"), encoding="utf-8") as file:
        assert len(file.read().splitlines()) == 50
    await item_pipeline.on_spider_close()
    await storage.close()


@pytest.mark.asyncio
async def test_recovery_keeps_multiple_namespaces(state_dir):
    class Alpha:
        pass

    class Beta:
        pass

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    first_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    second_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    first_pipeline.bind_storage(storage)
    second_pipeline.bind_storage(storage)
    await first_pipeline.on_spider_open()
    await second_pipeline.on_spider_open()

    first = Alpha()
    first.key = "a"
    await first_pipeline.process(first)

    second = Beta()
    second.key = "b"
    await second_pipeline.process(second)
    await first_pipeline.on_spider_close()
    await second_pipeline.on_spider_close()
    await storage.close()

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    first_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    second_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )
    first_pipeline.bind_storage(storage)
    second_pipeline.bind_storage(storage)
    await first_pipeline.on_spider_open()
    await second_pipeline.on_spider_open()
    await first_pipeline._ensure_output("Alpha")
    await second_pipeline._ensure_output("Beta")
    assert storage.is_item_committed("a")
    assert storage.is_item_committed("b")
    await storage.close()


@pytest.mark.asyncio
async def test_corrupt_database_restores_from_checkpoint_backup(state_dir):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    fingerprint = "a" * 64
    await storage.mark_request_done(fingerprint)
    await storage.checkpoint(force=True)
    await storage.close()

    with open(os.path.join(state_dir, "state.db"), "wb") as file:
        file.write(b"not a sqlite database" * 16)
    for suffix in ("-wal", "-shm"):
        sidecar = os.path.join(state_dir, "state.db" + suffix)
        if os.path.exists(sidecar):
            os.remove(sidecar)

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    assert storage.is_request_known(fingerprint)
    assert os.path.exists(os.path.join(state_dir, "state.db.bak"))
    quarantined = [
        name for name in os.listdir(state_dir) if name.endswith(".corrupt")
    ] or [name for name in os.listdir(state_dir) if ".corrupt-" in name]
    assert quarantined
    await storage.close()


@pytest.mark.asyncio
async def test_corrupt_database_without_backup_raises(state_dir):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    await storage.close()
    os.remove(os.path.join(state_dir, "state.db.bak"))

    with open(os.path.join(state_dir, "state.db"), "wb") as file:
        file.write(b"garbage state that can never be read")
    with pytest.raises(StateCorruptedError):
        await StateStorage(state_dir, mode=Mode.RESUME).open()


@pytest.mark.asyncio
async def test_full_mode_and_reset_wipe_everything(state_dir, tmp_path, item_cls):
    output_dir = str(tmp_path / "out")
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=output_dir,
        idempotency_key=lambda item: item.key,
    )
    pipeline.bind_storage(storage)
    await pipeline.on_spider_open()
    item = item_cls()
    item.key = "only-item"
    await pipeline.process(item)
    await storage.mark_request_done("b" * 64)
    await storage.set_high_water_mark(9)
    output_path = os.path.join(output_dir, "Item.jsonl")
    assert os.path.exists(output_path)
    await pipeline.on_spider_close()
    await storage.close()

    storage = await StateStorage(state_dir, mode=Mode.FULL).open()
    assert storage.processed_request_count == 0
    assert storage.committed_item_count == 0
    assert await storage.get_high_water_mark() == 0
    assert not os.path.exists(output_path)
    assert os.path.exists(os.path.join(state_dir, "state.db"))
    await storage.close()

    StateStorage.reset(state_dir)
    assert os.path.exists(state_dir)
    assert not os.listdir(state_dir)


@pytest.mark.asyncio
async def test_high_water_mark_survives_resume(state_dir):
    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    assert await storage.get_high_water_mark() == 0
    await storage.set_high_water_mark(7)
    await storage.save_meta(custom=(1, "two"))
    await storage.close()

    storage = await StateStorage(state_dir, mode=Mode.RESUME).open()
    assert await storage.get_high_water_mark() == 7
    assert await storage.get_meta("custom") == (1, "two")
    assert await storage.get_meta("missing", "default") == "default"
    await storage.close()


@pytest.mark.asyncio
async def test_ant_lifecycle_resume_and_failure_retry(monkeypatch, tmp_path, item_cls):
    from ant_nest import ant as ant_module

    state_dir = str(tmp_path / "ant-state")
    monkeypatch.setattr(
        ant_module.settings,
        "PERSISTENT_STATE",
        {
            "enabled": True,
            "dir": state_dir,
            "mode": "resume",
            "checkpoint_interval": 2,
        },
    )

    request_pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    item_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )

    class TestAnt(Ant):
        request_pipelines = [request_pipeline]
        item_pipelines = [item_pipeline]

        async def run(self):
            await self.request("http://test.com/ok")
            item = item_cls()
            item.key = "k-ok"
            await self.collect(item)
            try:
                await self.request("http://test.com/fail")
            except Exception:
                pass

    fail_should_raise = {"value": True}

    async def fake_send(self, request, auth=None, stream=False):
        if fail_should_raise["value"] and "fail" in str(request.url):
            raise httpx.ConnectError("boom")
        return httpx.Response(200, request=request, content=b"ok")

    monkeypatch.setattr(httpx.AsyncClient, "send", fake_send)

    ant = TestAnt()
    assert ant.state_storage is not None
    await ant.main()

    ok_fp = request_fingerprint(httpx.Request("GET", "http://test.com/ok"))
    fail_fp = request_fingerprint(httpx.Request("GET", "http://test.com/fail"))
    assert ant.state_storage.is_request_known(ok_fp)
    assert not ant.state_storage.is_request_known(fail_fp)
    assert ant.state_storage.is_item_committed("k-ok")

    # second process, resume mode: ok request/items are skipped, fail retries
    second_request_pipeline = pls.PersistentRequestDuplicateFilterPipeline()
    second_item_pipeline = pls.PersistentItemJsonDumpPipeline(
        to_dict=lambda item: item.__dict__,
        file_dir=os.path.join(state_dir, "items"),
        idempotency_key=lambda item: item.key,
    )

    class ResumeAnt(Ant):
        request_pipelines = [second_request_pipeline]
        item_pipelines = [second_item_pipeline]

        async def run(self):
            with pytest.raises(Dropped):
                await self.request("http://test.com/ok")
            retry_item = item_cls()
            retry_item.key = "k-ok"
            with pytest.raises(Dropped):
                await self.collect(retry_item)
            await self.request("http://test.com/fail")
            await self.request("http://test.com/new")

    fail_should_raise["value"] = False
    resume_ant = ResumeAnt()
    await resume_ant.main()
    assert resume_ant.state_storage.is_request_known(fail_fp)
    new_fp = request_fingerprint(httpx.Request("GET", "http://test.com/new"))
    assert resume_ant.state_storage.is_request_known(new_fp)
    with open(os.path.join(state_dir, "items", "Item.jsonl"), encoding="utf-8") as file:
        assert len(file.read().splitlines()) == 1


@pytest.mark.asyncio
async def test_persistent_state_disabled_by_default(monkeypatch):
    from ant_nest import ant as ant_module

    monkeypatch.delattr(ant_module.settings, "PERSISTENT_STATE", raising=False)

    class PlainAnt(Ant):
        async def run(self):
            return None

    ant = PlainAnt()
    assert ant.state_storage is None
    await ant.open()
    await ant.close()

    # legacy in-memory pipelines keep their original behavior
    request_pipeline = pls.RequestDuplicateFilterPipeline()
    request = httpx.Request("GET", "http://test.com")
    assert request_pipeline.process(request) is request
    with pytest.raises(Dropped):
        request_pipeline.process(request)
    item_pipeline = pls.ItemJsonDumpPipeline(to_dict=lambda item: item.__dict__)
    item = type("Item", (), {})()
    item.name = "legacy"
    assert item_pipeline.process(item) is item
    assert item_pipeline.data["Item"][0]["name"] == "legacy"
