from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum as SqlEnum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    delete,
    func,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_id: int = Field(..., alias="API_ID")
    api_hash: str = Field(..., alias="API_HASH")
    user_session_name: str = Field("user_session", alias="USER_SESSION_NAME")

    bot_token: str = Field(..., alias="BOT_TOKEN")
    bot_session_name: str = Field("admin_bot", alias="BOT_SESSION_NAME")

    database_url: str = Field(..., alias="DATABASE_URL")
    admin_ids: List[int] = Field(default_factory=list, alias="ADMIN_IDS")

    poll_interval_seconds: float = Field(1.0, alias="POLL_INTERVAL_SECONDS")
    max_retries: int = Field(5, alias="MAX_RETRIES")
    album_flush_seconds: float = Field(1.8, alias="ALBUM_FLUSH_SECONDS")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    @field_validator("admin_ids", mode="before")
    @classmethod
    def parse_admin_ids(cls, value: object) -> List[int]:
        if value is None:
            return []
        if isinstance(value, list):
            return [int(v) for v in value]
        if isinstance(value, str):
            return [int(v.strip()) for v in value.split(",") if v.strip()]
        raise ValueError("ADMIN_IDS must be comma separated.")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


logger = logging.getLogger("medi_save_auto_forward")


class Base(DeclarativeBase):
    pass


class QueueStatus(str, Enum):
    pending = "pending"
    sending = "sending"
    sent = "sent"
    failed = "failed"


class DeliveryStatus(str, Enum):
    pending = "pending"
    sent = "sent"
    failed = "failed"
    skipped = "skipped"


class Destination(Base):
    __tablename__ = "destinations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class QueueItem(Base):
    __tablename__ = "queue_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_message_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    media_group_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    file_unique_id: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    file_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    file_type: Mapped[str] = mapped_column(String(32), nullable=False)
    caption: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    file_size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    status: Mapped[QueueStatus] = mapped_column(SqlEnum(QueueStatus), default=QueueStatus.pending, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    deliveries: Mapped[list["Delivery"]] = relationship(back_populates="queue_item", cascade="all, delete-orphan")


class Delivery(Base):
    __tablename__ = "deliveries"
    __table_args__ = (UniqueConstraint("queue_item_id", "destination_id", name="uq_delivery_queue_destination"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    queue_item_id: Mapped[int] = mapped_column(ForeignKey("queue_items.id", ondelete="CASCADE"), nullable=False, index=True)
    destination_id: Mapped[int] = mapped_column(ForeignKey("destinations.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[DeliveryStatus] = mapped_column(SqlEnum(DeliveryStatus), default=DeliveryStatus.pending, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    queue_item: Mapped[QueueItem] = relationship(back_populates="deliveries")


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    delay_seconds: Mapped[float] = mapped_column(default=1.0, nullable=False)
    max_file_size_mb: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    caption_prefix: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auto_delete_saved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    duplicate_protection: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Database:
    def __init__(self, settings: Settings) -> None:
        self.engine: AsyncEngine = create_async_engine(settings.database_url, future=True, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(bind=self.engine, expire_on_commit=False, autoflush=False, class_=AsyncSession)

    async def create_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self) -> None:
        await self.engine.dispose()

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncSession:
        async with self.session_factory() as session:
            yield session


@dataclass(slots=True)
class MediaPayload:
    source_chat_id: int
    source_message_id: int
    media_group_id: Optional[str]
    file_unique_id: Optional[str]
    file_id: Optional[str]
    file_type: str
    caption: Optional[str]
    file_size: Optional[int]

class Repository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ensure_runtime_settings(self) -> RuntimeSetting:
        row = await self.session.get(RuntimeSetting, 1)
        if row:
            return row
        row = RuntimeSetting(id=1)
        self.session.add(row)
        await self.session.commit()
        return row

    async def get_runtime_settings(self) -> RuntimeSetting:
        row = await self.session.get(RuntimeSetting, 1)
        return row if row else await self.ensure_runtime_settings()

    async def add_destination(self, chat_id: int) -> Destination:
        row = await self.session.scalar(select(Destination).where(Destination.chat_id == chat_id))
        if row:
            row.is_enabled = True
            await self.session.commit()
            return row
        row = Destination(chat_id=chat_id, is_enabled=True)
        self.session.add(row)
        await self.session.commit()
        return row

    async def disable_destination(self, chat_id: int) -> bool:
        result = await self.session.execute(update(Destination).where(Destination.chat_id == chat_id).values(is_enabled=False))
        await self.session.commit()
        return (result.rowcount or 0) > 0

    async def list_destinations(self) -> list[Destination]:
        rows = await self.session.scalars(select(Destination).order_by(Destination.id.asc()))
        return list(rows)

    async def enqueue_media(self, payload: MediaPayload, skip_if_duplicate: bool = True) -> Optional[QueueItem]:
        if skip_if_duplicate and payload.file_unique_id:
            duplicate = await self.session.scalar(
                select(QueueItem).where(
                    QueueItem.file_unique_id == payload.file_unique_id,
                    QueueItem.status.in_([QueueStatus.pending, QueueStatus.sending, QueueStatus.sent]),
                )
            )
            if duplicate:
                return None

        existing = await self.session.scalar(select(QueueItem).where(QueueItem.source_message_id == payload.source_message_id))
        if existing:
            return None

        dests = list(await self.session.scalars(select(Destination).where(Destination.is_enabled.is_(True))))
        if not dests:
            return None

        item = QueueItem(
            source_chat_id=payload.source_chat_id,
            source_message_id=payload.source_message_id,
            media_group_id=payload.media_group_id,
            file_unique_id=payload.file_unique_id,
            file_id=payload.file_id,
            file_type=payload.file_type,
            caption=payload.caption,
            file_size=payload.file_size,
            status=QueueStatus.pending,
        )
        self.session.add(item)
        await self.session.flush()
        for d in dests:
            self.session.add(Delivery(queue_item_id=item.id, destination_id=d.id, status=DeliveryStatus.pending))

        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            return None
        return item

    async def fetch_next_item(self) -> Optional[QueueItem]:
        item = await self.session.scalar(
            select(QueueItem)
            .where(QueueItem.status.in_([QueueStatus.pending, QueueStatus.failed]))
            .order_by(QueueItem.created_at.asc())
            .limit(1)
        )
        if not item:
            return None
        full = await self.session.scalar(
            select(QueueItem).where(QueueItem.id == item.id).options(selectinload(QueueItem.deliveries))
        )
        if not full:
            return None
        full.status = QueueStatus.sending
        await self.session.commit()
        return full

    async def refresh_item(self, item_id: int) -> Optional[QueueItem]:
        return await self.session.scalar(select(QueueItem).where(QueueItem.id == item_id).options(selectinload(QueueItem.deliveries)))

    async def increment_attempt(self, item_id: int, error: str) -> None:
        row = await self.session.get(QueueItem, item_id)
        if not row:
            return
        row.attempts += 1
        row.error_text = error
        await self.session.commit()

    async def update_delivery(self, delivery_id: int, status: DeliveryStatus, error_text: Optional[str] = None) -> None:
        values = {"status": status, "error_text": error_text}
        if status == DeliveryStatus.sent:
            values["sent_at"] = datetime.now(timezone.utc)
        await self.session.execute(update(Delivery).where(Delivery.id == delivery_id).values(**values))
        await self.session.commit()

    async def finalize_item(self, row: QueueItem, max_retries: int) -> QueueItem:
        sent = sum(1 for d in row.deliveries if d.status == DeliveryStatus.sent)
        failed = sum(1 for d in row.deliveries if d.status == DeliveryStatus.failed)
        if row.deliveries and sent == len(row.deliveries):
            row.status = QueueStatus.sent
            row.sent_at = datetime.now(timezone.utc)
            row.error_text = None
        elif row.attempts >= max_retries or failed > 0:
            row.status = QueueStatus.failed
        else:
            row.status = QueueStatus.pending
        await self.session.commit()
        return row

    async def stats(self) -> dict[str, int]:
        total = await self.session.scalar(select(func.count(QueueItem.id)))
        pending = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.pending))
        sent = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.sent))
        failed = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.failed))
        size = await self.session.scalar(select(func.coalesce(func.sum(QueueItem.file_size), 0)))
        return {
            "total": int(total or 0),
            "pending": int(pending or 0),
            "sent": int(sent or 0),
            "failed": int(failed or 0),
            "size": int(size or 0),
        }

    async def clear_queue(self) -> int:
        result = await self.session.execute(delete(QueueItem))
        await self.session.commit()
        return int(result.rowcount or 0)


def extract_media_payload(message: Message) -> Optional[dict[str, object]]:
    if message.photo:
        return {"file_type": "photo", "file_id": message.photo.file_id, "file_unique_id": message.photo.file_unique_id, "file_size": message.photo.file_size}
    if message.video:
        return {"file_type": "video", "file_id": message.video.file_id, "file_unique_id": message.video.file_unique_id, "file_size": message.video.file_size}
    if message.document:
        return {"file_type": "document", "file_id": message.document.file_id, "file_unique_id": message.document.file_unique_id, "file_size": message.document.file_size}
    if message.audio:
        return {"file_type": "audio", "file_id": message.audio.file_id, "file_unique_id": message.audio.file_unique_id, "file_size": message.audio.file_size}
    return None

@dataclass(slots=True)
class AlbumBucket:
    messages: List[Message]
    flush_task: asyncio.Task[None]


class SavedMessagesListener:
    def __init__(self, user_client: Client, db: Database, settings: Settings) -> None:
        self.user_client = user_client
        self.db = db
        self.settings = settings
        self.album_buckets: Dict[str, AlbumBucket] = {}
        self.album_lock = asyncio.Lock()

    def register_handlers(self) -> None:
        @self.user_client.on_message(filters.chat("me") & (filters.photo | filters.video | filters.document | filters.audio))
        async def _on_media(_: Client, message: Message) -> None:
            await self.handle_message(message)

    async def handle_message(self, message: Message) -> None:
        if message.media_group_id:
            await self.buffer_album(str(message.media_group_id), message)
            return
        await self.enqueue_message(message)

    async def buffer_album(self, media_group_id: str, message: Message) -> None:
        async with self.album_lock:
            bucket = self.album_buckets.get(media_group_id)
            if bucket:
                bucket.messages.append(message)
                if not bucket.flush_task.done():
                    bucket.flush_task.cancel()
            else:
                bucket = AlbumBucket(messages=[message], flush_task=asyncio.create_task(asyncio.sleep(0)))
            bucket.flush_task = asyncio.create_task(self.flush_album(media_group_id))
            self.album_buckets[media_group_id] = bucket

    async def flush_album(self, media_group_id: str) -> None:
        try:
            await asyncio.sleep(self.settings.album_flush_seconds)
            async with self.album_lock:
                bucket = self.album_buckets.pop(media_group_id, None)
            if not bucket:
                return
            for message in sorted(bucket.messages, key=lambda m: m.id):
                await self.enqueue_message(message)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Album flush failed: %s", media_group_id)

    async def enqueue_message(self, message: Message) -> None:
        media = extract_media_payload(message)
        if not media:
            return
        async with self.db.session() as session:
            repo = Repository(session)
            settings = await repo.get_runtime_settings()
            if not settings.is_enabled:
                return
            if settings.max_file_size_mb > 0:
                limit = settings.max_file_size_mb * 1024 * 1024
                if media["file_size"] and int(media["file_size"]) > limit:
                    return

            payload = MediaPayload(
                source_chat_id=message.chat.id,
                source_message_id=message.id,
                media_group_id=str(message.media_group_id) if message.media_group_id else None,
                file_unique_id=str(media["file_unique_id"]) if media["file_unique_id"] else None,
                file_id=str(media["file_id"]) if media["file_id"] else None,
                file_type=str(media["file_type"]),
                caption=message.caption,
                file_size=int(media["file_size"]) if media["file_size"] else None,
            )
            await repo.enqueue_media(payload, skip_if_duplicate=settings.duplicate_protection)


class QueueWorker:
    def __init__(self, user_client: Client, db: Database, settings: Settings) -> None:
        self.user_client = user_client
        self.db = db
        self.settings = settings
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                async with self.db.session() as session:
                    repo = Repository(session)
                    runtime = await repo.get_runtime_settings()
                    if not runtime.is_enabled:
                        await asyncio.sleep(self.settings.poll_interval_seconds)
                        continue

                    item = await repo.fetch_next_item()
                    if not item:
                        await asyncio.sleep(self.settings.poll_interval_seconds)
                        continue

                    destinations = list(await session.scalars(select(Destination).where(Destination.is_enabled.is_(True))))
                    for delivery, dest in zip(item.deliveries, destinations):
                        if delivery.status == DeliveryStatus.sent:
                            continue
                        caption = item.caption
                        if runtime.caption_prefix:
                            caption = f"{runtime.caption_prefix}\n\n{item.caption}" if item.caption else runtime.caption_prefix
                        await asyncio.sleep(max(0.0, runtime.delay_seconds))
                        await self.send_one(repo, item.id, delivery.id, item.source_chat_id, item.source_message_id, dest.chat_id, caption)

                    fresh = await repo.refresh_item(item.id)
                    if not fresh:
                        continue
                    done = await repo.finalize_item(fresh, self.settings.max_retries)
                    if done.status == QueueStatus.sent and runtime.auto_delete_saved:
                        with contextlib.suppress(Exception):
                            await self.user_client.delete_messages("me", done.source_message_id)
            except Exception:
                logger.exception("Worker loop failed")
                await asyncio.sleep(self.settings.poll_interval_seconds)

    async def send_one(self, repo: Repository, item_id: int, delivery_id: int, from_chat: int, msg_id: int, to_chat: int, caption: Optional[str]) -> None:
        try:
            await self.user_client.copy_message(chat_id=to_chat, from_chat_id=from_chat, message_id=msg_id, caption=caption)
            await repo.update_delivery(delivery_id, DeliveryStatus.sent)
        except FloodWait as exc:
            await repo.update_delivery(delivery_id, DeliveryStatus.failed, f"FloodWait({exc.value})")
            await repo.increment_attempt(item_id, f"FloodWait({exc.value})")
            await asyncio.sleep(exc.value + 1)
        except RPCError as exc:
            await repo.update_delivery(delivery_id, DeliveryStatus.failed, str(exc))
            await repo.increment_attempt(item_id, str(exc))
        except Exception as exc:
            await repo.update_delivery(delivery_id, DeliveryStatus.failed, str(exc))
            await repo.increment_attempt(item_id, str(exc))

    async def stop(self) -> None:
        self.stop_event.set()


def register_admin_handlers(bot_client: Client, db: Database, admin_ids: set[int]) -> None:
    async def guard(message: Message) -> bool:
        uid = message.from_user.id if message.from_user else None
        if uid not in admin_ids:
            await message.reply_text("Unauthorized.")
            return False
        return True

    @bot_client.on_message(filters.command(["start", "help"]))
    async def help_cmd(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        await message.reply_text(
            "Commands:\n"
            "/enable | /disable\n"
            "/setdelay <seconds>\n"
            "/setmaxsize <mb>\n"
            "/setprefix <text> | /setprefix off\n"
            "/autodelete on|off\n"
            "/dupe on|off\n"
            "/adddest <chat_id>\n"
            "/removedest <chat_id>\n"
            "/listdest\n"
            "/stats\n"
            "/clearqueue"
        )

    @bot_client.on_message(filters.command("enable"))
    async def enable_cmd(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.is_enabled = True
            await session.commit()
        await message.reply_text("Enabled.")

    @bot_client.on_message(filters.command("disable"))
    async def disable_cmd(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.is_enabled = False
            await session.commit()
        await message.reply_text("Disabled.")

    @bot_client.on_message(filters.command("setdelay"))
    async def set_delay(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2:
            await message.reply_text("Usage: /setdelay <seconds>")
            return
        try:
            delay = float(message.command[1])
            if delay < 0:
                raise ValueError
        except ValueError:
            await message.reply_text("Invalid delay")
            return
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.delay_seconds = delay
            await session.commit()
        await message.reply_text(f"Delay updated: {delay}s")

    @bot_client.on_message(filters.command("setmaxsize"))
    async def set_max(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2:
            await message.reply_text("Usage: /setmaxsize <mb>")
            return
        try:
            mb = int(message.command[1])
            if mb < 0:
                raise ValueError
        except ValueError:
            await message.reply_text("Invalid mb")
            return
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.max_file_size_mb = mb
            await session.commit()
        await message.reply_text(f"Max size updated: {mb}MB")

    @bot_client.on_message(filters.command("setprefix"))
    async def set_prefix(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2:
            await message.reply_text("Usage: /setprefix <text> | /setprefix off")
            return
        content = message.text.split(maxsplit=1)[1].strip() if message.text else ""
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.caption_prefix = None if content.lower() == "off" else content
            await session.commit()
        await message.reply_text("Prefix updated.")

    @bot_client.on_message(filters.command("autodelete"))
    async def set_autodel(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2 or message.command[1].lower() not in {"on", "off"}:
            await message.reply_text("Usage: /autodelete on|off")
            return
        on = message.command[1].lower() == "on"
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.auto_delete_saved = on
            await session.commit()
        await message.reply_text(f"Auto delete: {'ON' if on else 'OFF'}")

    @bot_client.on_message(filters.command("dupe"))
    async def set_dupe(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2 or message.command[1].lower() not in {"on", "off"}:
            await message.reply_text("Usage: /dupe on|off")
            return
        on = message.command[1].lower() == "on"
        async with db.session() as session:
            repo = Repository(session)
            row = await repo.get_runtime_settings()
            row.duplicate_protection = on
            await session.commit()
        await message.reply_text(f"Duplicate protection: {'ON' if on else 'OFF'}")

    @bot_client.on_message(filters.command("adddest"))
    async def add_dest(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2:
            await message.reply_text("Usage: /adddest <chat_id>")
            return
        try:
            chat_id = int(message.command[1])
        except ValueError:
            await message.reply_text("chat_id must be integer")
            return
        async with db.session() as session:
            await Repository(session).add_destination(chat_id)
        await message.reply_text("Destination added.")

    @bot_client.on_message(filters.command("removedest"))
    async def remove_dest(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        if len(message.command) < 2:
            await message.reply_text("Usage: /removedest <chat_id>")
            return
        try:
            chat_id = int(message.command[1])
        except ValueError:
            await message.reply_text("chat_id must be integer")
            return
        async with db.session() as session:
            ok = await Repository(session).disable_destination(chat_id)
        await message.reply_text("Destination disabled." if ok else "Destination not found.")

    @bot_client.on_message(filters.command("listdest"))
    async def list_dest(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        async with db.session() as session:
            rows = await Repository(session).list_destinations()
        if not rows:
            await message.reply_text("No destinations.")
            return
        await message.reply_text("\n".join([f"{r.chat_id} [{'on' if r.is_enabled else 'off'}]" for r in rows]))

    @bot_client.on_message(filters.command("stats"))
    async def stats(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        async with db.session() as session:
            s = await Repository(session).stats()
        await message.reply_text(f"Total={s['total']} Pending={s['pending']} Sent={s['sent']} Failed={s['failed']} Size={s['size']} bytes")

    @bot_client.on_message(filters.command("clearqueue"))
    async def clear_queue(_: Client, message: Message) -> None:
        if not await guard(message):
            return
        async with db.session() as session:
            c = await Repository(session).clear_queue()
        await message.reply_text(f"Queue cleared: {c} rows")


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    db = Database(settings)
    await db.create_schema()
    async with db.session() as session:
        await Repository(session).ensure_runtime_settings()

    user_client = Client(
        name="user_session",
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        session_string=os.getenv("SESSION_STRING")
    )
    bot_client = Client(
        name=settings.bot_session_name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        bot_token=settings.bot_token,
    )

    listener = SavedMessagesListener(user_client, db, settings)
    listener.register_handlers()
    register_admin_handlers(bot_client, db, set(settings.admin_ids))
    worker = QueueWorker(user_client, db, settings)

    stop_event = asyncio.Event()

    def stop_handler() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_handler)

    await user_client.start()
    await bot_client.start()
    worker_task = asyncio.create_task(worker.run())
    logger.info("medi_save_auto_forward started")

    try:
        await stop_event.wait()
    finally:
        await worker.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await bot_client.stop()
        await user_client.stop()
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(run())
