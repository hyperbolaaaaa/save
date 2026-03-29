
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from html import escape
from typing import Dict, List, Optional
from pyrogram.enums import ParseMode
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
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
    user_session_string: Optional[str] = Field(default=None, alias="USER_SESSION_STRING")

    bot_token: str = Field(..., alias="BOT_TOKEN")
    bot_session_name: str = Field("admin_bot", alias="BOT_SESSION_NAME")

    database_url: str = Field(..., alias="DATABASE_URL")
    admin_ids: List[int] = Field(default_factory=list, alias="ADMIN_IDS")

    poll_interval_seconds: float = Field(1.0, alias="POLL_INTERVAL_SECONDS")
    max_retries: int = Field(5, alias="MAX_RETRIES")
    retry_backoff_seconds: float = Field(8.0, alias="RETRY_BACKOFF_SECONDS")
    album_flush_seconds: float = Field(1.8, alias="ALBUM_FLUSH_SECONDS")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    web_panel_enabled: bool = Field(False, alias="WEB_PANEL_ENABLED")
    web_panel_host: str = Field("0.0.0.0", alias="WEB_PANEL_HOST")
    web_panel_port: int = Field(8080, alias="WEB_PANEL_PORT")
    web_panel_token: Optional[str] = Field(default=None, alias="WEB_PANEL_TOKEN")

    @field_validator("admin_ids", mode="before")
    @classmethod
    def parse_admin_ids(cls, value: object) -> List[int]:
        if value is None:
            return []
        if isinstance(value, list):
            return [int(v) for v in value]
        if isinstance(value, str):
            return [int(v.strip()) for v in value.split(",") if v.strip()]
        raise ValueError("ADMIN_IDS must be comma-separated.")


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


class AdminRole(str, Enum):
    owner = "owner"
    admin = "admin"
    viewer = "viewer"


class Destination(Base):
    __tablename__ = "destinations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    deliveries: Mapped[list["Delivery"]] = relationship(back_populates="destination", cascade="all, delete-orphan")


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
    status: Mapped[QueueStatus] = mapped_column(SqlEnum(QueueStatus), default=QueueStatus.pending, nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    deliveries: Mapped[list["Delivery"]] = relationship(back_populates="queue_item", cascade="all, delete-orphan")


class Delivery(Base):
    __tablename__ = "deliveries"
    __table_args__ = (UniqueConstraint("queue_item_id", "destination_id", name="uq_delivery_queue_destination"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    queue_item_id: Mapped[int] = mapped_column(ForeignKey("queue_items.id", ondelete="CASCADE"), nullable=False, index=True)
    destination_id: Mapped[int] = mapped_column(ForeignKey("destinations.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[DeliveryStatus] = mapped_column(SqlEnum(DeliveryStatus), default=DeliveryStatus.pending, nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    queue_item: Mapped[QueueItem] = relationship(back_populates="deliveries")
    destination: Mapped[Destination] = relationship(back_populates="deliveries")


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    delay_seconds: Mapped[float] = mapped_column(default=1.0, nullable=False)
    max_file_size_mb: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    caption_prefix: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auto_delete_saved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    duplicate_protection: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class AdminUser(Base):
    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    role: Mapped[AdminRole] = mapped_column(SqlEnum(AdminRole), default=AdminRole.admin, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


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

    async def bootstrap_admins(self, admin_ids: List[int]) -> None:
        for user_id in admin_ids:
            existing = await self.session.scalar(select(AdminUser).where(AdminUser.tg_user_id == user_id))
            if existing:
                if not existing.is_active:
                    existing.is_active = True
                if existing.role != AdminRole.owner:
                    existing.role = AdminRole.owner
                continue
            self.session.add(AdminUser(tg_user_id=user_id, role=AdminRole.owner, is_active=True))
        await self.session.commit()

    async def get_admin(self, user_id: int) -> Optional[AdminUser]:
        return await self.session.scalar(
            select(AdminUser).where(AdminUser.tg_user_id == user_id, AdminUser.is_active.is_(True))
        )

    async def add_or_update_admin(self, user_id: int, role: AdminRole) -> AdminUser:
        row = await self.session.scalar(select(AdminUser).where(AdminUser.tg_user_id == user_id))
        if row:
            row.role = role
            row.is_active = True
            await self.session.commit()
            return row
        row = AdminUser(tg_user_id=user_id, role=role, is_active=True)
        self.session.add(row)
        await self.session.commit()
        return row

    async def remove_admin(self, user_id: int) -> bool:
        row = await self.session.scalar(select(AdminUser).where(AdminUser.tg_user_id == user_id))
        if not row:
            return False
        row.is_active = False
        await self.session.commit()
        return True

    async def list_admins(self) -> list[AdminUser]:
        rows = await self.session.scalars(
            select(AdminUser).where(AdminUser.is_active.is_(True)).order_by(AdminUser.created_at.asc())
        )
        return list(rows)

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

        for dest in dests:
            self.session.add(Delivery(queue_item_id=item.id, destination_id=dest.id, status=DeliveryStatus.pending))

        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            return None
        return item

    async def fetch_next_item(self) -> Optional[QueueItem]:
        now = datetime.now(timezone.utc)
        item = await self.session.scalar(
            select(QueueItem)
            .where(
                QueueItem.status == QueueStatus.pending,
                ((QueueItem.next_retry_at.is_(None)) | (QueueItem.next_retry_at <= now)),
            )
            .order_by(QueueItem.created_at.asc())
            .limit(1)
        )
        if not item:
            return None

        full = await self.session.scalar(
            select(QueueItem)
            .where(QueueItem.id == item.id)
            .options(selectinload(QueueItem.deliveries).selectinload(Delivery.destination))
        )
        if not full:
            return None

        full.status = QueueStatus.sending
        await self.session.commit()
        return full

    async def refresh_item(self, item_id: int) -> Optional[QueueItem]:
        return await self.session.scalar(
            select(QueueItem)
            .where(QueueItem.id == item_id)
            .options(selectinload(QueueItem.deliveries).selectinload(Delivery.destination))
        )

    async def update_delivery(self, delivery_id: int, status: DeliveryStatus, error_text: Optional[str] = None) -> None:
        values: dict[str, object] = {"status": status, "error_text": error_text}
        if status == DeliveryStatus.sent:
            values["sent_at"] = datetime.now(timezone.utc)
        if status == DeliveryStatus.failed:
            delivery = await self.session.get(Delivery, delivery_id)
            if delivery:
                values["attempts"] = delivery.attempts + 1
        await self.session.execute(update(Delivery).where(Delivery.id == delivery_id).values(**values))
        await self.session.commit()

    async def schedule_retry(self, item_id: int, error_text: str, max_retries: int, base_backoff: float) -> None:
        row = await self.session.get(QueueItem, item_id)
        if not row:
            return
        row.attempts += 1
        row.error_text = error_text

        if row.attempts >= max_retries:
            row.status = QueueStatus.failed
            row.next_retry_at = None
        else:
            delay = max(1.0, base_backoff * (2 ** max(0, row.attempts - 1)))
            row.status = QueueStatus.pending
            row.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        await self.session.commit()

    async def mark_item_sent(self, item_id: int) -> None:
        row = await self.session.get(QueueItem, item_id)
        if not row:
            return
        row.status = QueueStatus.sent
        row.sent_at = datetime.now(timezone.utc)
        row.error_text = None
        row.next_retry_at = None
        await self.session.commit()

    async def finalize_item(self, row: QueueItem) -> QueueStatus:
        sent_count = sum(1 for d in row.deliveries if d.status == DeliveryStatus.sent)
        retryable_count = sum(1 for d in row.deliveries if d.status in (DeliveryStatus.failed, DeliveryStatus.pending))

        if row.deliveries and sent_count == len(row.deliveries):
            await self.mark_item_sent(row.id)
            return QueueStatus.sent

        if retryable_count > 0:
            row.status = QueueStatus.pending
            await self.session.commit()
            return QueueStatus.pending

        row.status = QueueStatus.failed
        await self.session.commit()
        return QueueStatus.failed

    async def reset_delivery_failures(self, item_id: int) -> None:
        await self.session.execute(
            update(Delivery)
            .where(Delivery.queue_item_id == item_id, Delivery.status == DeliveryStatus.failed)
            .values(status=DeliveryStatus.pending, error_text=None)
        )
        await self.session.commit()

    async def retry_failed(self) -> int:
        result = await self.session.execute(
            update(QueueItem)
            .where(QueueItem.status == QueueStatus.failed)
            .values(status=QueueStatus.pending, attempts=0, error_text=None, next_retry_at=None)
        )
        await self.session.execute(
            update(Delivery)
            .where(Delivery.status == DeliveryStatus.failed)
            .values(status=DeliveryStatus.pending, attempts=0, error_text=None)
        )
        await self.session.commit()
        return int(result.rowcount or 0)

    async def stats(self) -> dict[str, int]:
        total = await self.session.scalar(select(func.count(QueueItem.id)))
        pending = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.pending))
        sent = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.sent))
        failed = await self.session.scalar(select(func.count(QueueItem.id)).where(QueueItem.status == QueueStatus.failed))
        size = await self.session.scalar(select(func.coalesce(func.sum(QueueItem.file_size), 0)))
        admins = await self.session.scalar(select(func.count(AdminUser.id)).where(AdminUser.is_active.is_(True)))
        return {
            "total": int(total or 0),
            "pending": int(pending or 0),
            "sent": int(sent or 0),
            "failed": int(failed or 0),
            "size": int(size or 0),
            "admins": int(admins or 0),
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
            runtime = await repo.get_runtime_settings()
            if not runtime.is_enabled:
                return

            if runtime.max_file_size_mb > 0:
                limit = runtime.max_file_size_mb * 1024 * 1024
                if media["file_size"] and int(media["file_size"]) > limit:
                    logger.info("Skipped by size limit: %s", message.id)
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
            queued = await repo.enqueue_media(payload, skip_if_duplicate=runtime.duplicate_protection)
            if queued:
                logger.info("Queued message_id=%s queue_id=%s", message.id, queued.id)


class QueueWorker:
    def __init__(self, user_client: Client, db: Database, settings: Settings) -> None:
        self.user_client = user_client
        self.db = db
        self.settings = settings
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("Queue worker started")
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

                    had_failure = False
                    for delivery in item.deliveries:
                        if delivery.status == DeliveryStatus.sent:
                            continue

                        destination = delivery.destination
                        if destination is None or not destination.is_enabled:
                            await repo.update_delivery(delivery.id, DeliveryStatus.skipped, "Destination disabled")
                            continue

                        caption = item.caption
                        if runtime.caption_prefix:
                            caption = f"{runtime.caption_prefix}\n\n{item.caption}" if item.caption else runtime.caption_prefix

                        await asyncio.sleep(max(0.0, runtime.delay_seconds))
                        ok, err = await self.send_one(item.source_chat_id, item.source_message_id, destination.chat_id, caption)
                        if ok:
                            await repo.update_delivery(delivery.id, DeliveryStatus.sent)
                        else:
                            had_failure = True
                            await repo.update_delivery(delivery.id, DeliveryStatus.failed, err)

                    fresh = await repo.refresh_item(item.id)
                    if not fresh:
                        continue

                    if had_failure:
                        await repo.schedule_retry(
                            item.id,
                            error_text="Delivery failure. Auto-retry scheduled.",
                            max_retries=self.settings.max_retries,
                            base_backoff=self.settings.retry_backoff_seconds,
                        )
                        await repo.reset_delivery_failures(item.id)
                    else:
                        final_status = await repo.finalize_item(fresh)
                        if final_status == QueueStatus.sent and runtime.auto_delete_saved:
                            with contextlib.suppress(Exception):
                                await self.user_client.delete_messages("me", item.source_message_id)
            except Exception:
                logger.exception("Worker loop failed")
                await asyncio.sleep(self.settings.poll_interval_seconds)

    async def send_one(
        self,
        from_chat: int,
        msg_id: int,
        to_chat: int,
        caption: Optional[str],
    ) -> tuple[bool, Optional[str]]:
        try:
            await self.user_client.copy_message(chat_id=to_chat, from_chat_id=from_chat, message_id=msg_id, caption=caption)
            return True, None
        except FloodWait as exc:
            await asyncio.sleep(exc.value + 1)
            return False, f"FloodWait({exc.value})"
        except RPCError as exc:
            return False, str(exc)
        except Exception as exc:
            return False, str(exc)

    async def stop(self) -> None:
        self.stop_event.set()


def register_admin_handlers(bot_client: Client, db: Database, settings: Settings) -> None:
    input_state: dict[int, str] = {}

    def main_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⚡ Toggle", callback_data="ui:toggle"), InlineKeyboardButton("🔄 Refresh", callback_data="ui:home")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="ui:settings"), InlineKeyboardButton("🎯 Targets", callback_data="ui:targets")],
                [InlineKeyboardButton("📦 Queue", callback_data="ui:queue"), InlineKeyboardButton("👥 Admins", callback_data="ui:admins")],
                [InlineKeyboardButton("🌐 Web Panel Link", callback_data="ui:webpanel")],
            ]
        )

    def settings_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⏱️ Set Delay", callback_data="ui:set_delay"), InlineKeyboardButton("📦 Set Max Size", callback_data="ui:set_maxsize")],
                [InlineKeyboardButton("📝 Set Prefix", callback_data="ui:set_prefix"), InlineKeyboardButton("🧹 Toggle AutoDelete", callback_data="ui:toggle_autodel")],
                [InlineKeyboardButton("🛡️ Toggle Dupe Protection", callback_data="ui:toggle_dupe")],
                [InlineKeyboardButton("⬅️ Back", callback_data="ui:home")],
            ]
        )

    def targets_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ Add Target", callback_data="ui:add_target"), InlineKeyboardButton("➖ Remove Target", callback_data="ui:remove_target")],
                [InlineKeyboardButton("📋 List Targets", callback_data="ui:list_targets")],
                [InlineKeyboardButton("⬅️ Back", callback_data="ui:home")],
            ]
        )

    def queue_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔁 Retry Failed", callback_data="ui:retry_failed"), InlineKeyboardButton("🗑️ Clear Queue", callback_data="ui:clear_queue")],
                [InlineKeyboardButton("⬅️ Back", callback_data="ui:home")],
            ]
        )

    def admins_kb() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ Add/Update Admin", callback_data="ui:add_admin"), InlineKeyboardButton("➖ Remove Admin", callback_data="ui:remove_admin")],
                [InlineKeyboardButton("📋 List Admins", callback_data="ui:list_admins")],
                [InlineKeyboardButton("⬅️ Back", callback_data="ui:home")],
            ]
        )

    async def get_admin(user_id: int) -> Optional[AdminUser]:
        async with db.session() as session:
            return await Repository(session).get_admin(user_id)

    async def guard_user(user_id: Optional[int], min_role: AdminRole = AdminRole.admin) -> tuple[bool, Optional[AdminUser], str]:
        if user_id is None:
            return False, None, "🛑 Unauthorized"
        rec = await get_admin(user_id)
        if rec is None:
            return False, None, "🛑 Unauthorized"
        order = {AdminRole.viewer: 1, AdminRole.admin: 2, AdminRole.owner: 3}
        if order[rec.role] < order[min_role]:
            return False, rec, "🚫 Permission denied"
        return True, rec, ""

    async def build_status_text() -> str:
        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()
            s = await repo.stats()
        return (
            "🤖 <b>Media Auto-Forward Control Center</b>\n"
            f"Status: {'✅ Enabled' if runtime.is_enabled else '⏸️ Disabled'}\n"
            f"⏱️ Delay: {runtime.delay_seconds}s | 📦 Max: {runtime.max_file_size_mb}MB\n"
            f"📝 Prefix: {(runtime.caption_prefix or '(none)')}\n"
            f"🧹 AutoDelete: {'ON' if runtime.auto_delete_saved else 'OFF'} | 🛡️ Dupe: {'ON' if runtime.duplicate_protection else 'OFF'}\n\n"
            f"📊 Total: {s['total']} | ⏳ Pending: {s['pending']} | ✅ Sent: {s['sent']} | ❌ Failed: {s['failed']}\n"
            f"👥 Admins: {s['admins']} | 💾 Size: {s['size']} bytes"
        )

    async def send_home(message: Message) -> None:
        await message.reply_text(await build_status_text(), reply_markup=main_kb(), parse_mode=ParseMode.HTML)

    @bot_client.on_message(filters.command(["start", "help"]))
    async def start_cmd(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        allowed, _, err = await guard_user(user_id, AdminRole.viewer)
        if not allowed:
            await message.reply_text(err)
            return
        await send_home(message)

    @bot_client.on_callback_query(filters.regex("^ui:"))
    async def callbacks(_: Client, query: CallbackQuery) -> None:
        user_id = query.from_user.id if query.from_user else None
        data = query.data or ""

        min_role = AdminRole.viewer
        if data in {"ui:toggle", "ui:set_delay", "ui:set_maxsize", "ui:set_prefix", "ui:toggle_autodel", "ui:toggle_dupe", "ui:add_target", "ui:remove_target", "ui:retry_failed"}:
            min_role = AdminRole.admin
        if data in {"ui:clear_queue", "ui:add_admin", "ui:remove_admin", "ui:list_admins"}:
            min_role = AdminRole.owner

        allowed, _, err = await guard_user(user_id, min_role)
        if not allowed:
            await query.answer(err, show_alert=True)
            return

        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()

            if data == "ui:home":
                await query.message.edit_text(await build_status_text(), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:settings":
                await query.message.edit_text("⚙️ <b>Settings Menu</b>", reply_markup=settings_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:targets":
                await query.message.edit_text("🎯 <b>Targets Menu</b>", reply_markup=targets_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:queue":
                await query.message.edit_text("📦 <b>Queue Menu</b>", reply_markup=queue_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:admins":
                await query.message.edit_text("👥 <b>Admins Menu</b>", reply_markup=admins_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:toggle":
                runtime.is_enabled = not runtime.is_enabled
                await session.commit()
                await query.message.edit_text(await build_status_text(), reply_markup=main_kb(), parse_mode=ParseMode.HTML)
            elif data == "ui:toggle_autodel":
                runtime.auto_delete_saved = not runtime.auto_delete_saved
                await session.commit()
                await query.answer("Updated ✅")
            elif data == "ui:toggle_dupe":
                runtime.duplicate_protection = not runtime.duplicate_protection
                await session.commit()
                await query.answer("Updated ✅")
            elif data == "ui:set_delay":
                input_state[user_id] = "set_delay"
                await query.message.reply_text("⏱️ Send new delay in seconds (example: 1.5)")
            elif data == "ui:set_maxsize":
                input_state[user_id] = "set_maxsize"
                await query.message.reply_text("📦 Send max size in MB (0 to disable)")
            elif data == "ui:set_prefix":
                input_state[user_id] = "set_prefix"
                await query.message.reply_text("📝 Send prefix text (or send `off`)", parse_mode="markdown")
            elif data == "ui:add_target":
                input_state[user_id] = "add_target"
                await query.message.reply_text("➕ Send target chat_id to add")
            elif data == "ui:remove_target":
                input_state[user_id] = "remove_target"
                await query.message.reply_text("➖ Send target chat_id to disable")
            elif data == "ui:list_targets":
                rows = await repo.list_destinations()
                if not rows:
                    await query.message.reply_text("📭 No targets configured")
                else:
                    txt = "\n".join([f"{'✅' if r.is_enabled else '⏸️'} {r.chat_id}" for r in rows])
                    await query.message.reply_text(f"🎯 Targets\n{txt}")
            elif data == "ui:retry_failed":
                count = await repo.retry_failed()
                await query.message.reply_text(f"🔁 Requeued failed items: {count}")
            elif data == "ui:clear_queue":
                count = await repo.clear_queue()
                await query.message.reply_text(f"🗑️ Queue cleared: {count} rows")
            elif data == "ui:add_admin":
                input_state[user_id] = "add_admin"
                await query.message.reply_text("👥 Send: <user_id> <owner|admin|viewer>")
            elif data == "ui:remove_admin":
                input_state[user_id] = "remove_admin"
                await query.message.reply_text("👥 Send admin user_id to remove")
            elif data == "ui:list_admins":
                rows = await repo.list_admins()
                if not rows:
                    await query.message.reply_text("📭 No active admins")
                else:
                    txt = "\n".join([f"👤 {r.tg_user_id} - {r.role.value}" for r in rows])
                    await query.message.reply_text(f"👥 Admins\n{txt}")
            elif data == "ui:webpanel":
                if not settings.web_panel_enabled:
                    await query.message.reply_text("🌐 Web panel disabled. Set WEB_PANEL_ENABLED=true")
                else:
                    token_part = f"?t={settings.web_panel_token}" if settings.web_panel_token else ""
                    await query.message.reply_text(f"🌐 Panel: http://{settings.web_panel_host}:{settings.web_panel_port}/{token_part}")

        await query.answer()

    @bot_client.on_message(filters.private & filters.text & ~filters.command(["start", "help"]))
    async def text_input(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if user_id is None or user_id not in input_state:
            return

        action = input_state.pop(user_id)
        text = (message.text or "").strip()

        min_role = AdminRole.admin if action in {"set_delay", "set_maxsize", "set_prefix", "add_target", "remove_target"} else AdminRole.owner
        allowed, _, err = await guard_user(user_id, min_role)
        if not allowed:
            await message.reply_text(err)
            return

        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()
            try:
                if action == "set_delay":
                    delay = float(text)
                    if delay < 0:
                        raise ValueError
                    runtime.delay_seconds = delay
                    await session.commit()
                    await message.reply_text(f"⏱️ Delay updated: {delay}s")
                elif action == "set_maxsize":
                    mb = int(text)
                    if mb < 0:
                        raise ValueError
                    runtime.max_file_size_mb = mb
                    await session.commit()
                    await message.reply_text(f"📦 Max size updated: {mb} MB")
                elif action == "set_prefix":
                    runtime.caption_prefix = None if text.lower() == "off" else text
                    await session.commit()
                    await message.reply_text("📝 Prefix updated")
                elif action == "add_target":
                    chat_id = int(text)
                    await repo.add_destination(chat_id)
                    await message.reply_text(f"🎯 Target added: {chat_id}")
                elif action == "remove_target":
                    chat_id = int(text)
                    ok = await repo.disable_destination(chat_id)
                    await message.reply_text("🧯 Target disabled" if ok else "❌ Target not found")
                elif action == "add_admin":
                    parts = text.split()
                    if len(parts) != 2:
                        raise ValueError
                    uid = int(parts[0])
                    role = AdminRole(parts[1].lower())
                    row = await repo.add_or_update_admin(uid, role)
                    await message.reply_text(f"👥 Admin updated: {row.tg_user_id} ({row.role.value})")
                elif action == "remove_admin":
                    uid = int(text)
                    ok = await repo.remove_admin(uid)
                    await message.reply_text("✅ Admin removed" if ok else "❌ Admin not found")
            except Exception:
                await message.reply_text("⚠️ Invalid input. Please use the requested format.")
def build_dashboard_html(stats: dict[str, int], runtime: RuntimeSetting, token_suffix: str) -> str:
    prefix = escape(runtime.caption_prefix or "(none)")
    state = "✅ Enabled" if runtime.is_enabled else "⏸️ Disabled"
    return f"""
    <html><head><title>Media Auto Forward Panel</title>
    <style>
      body {{ font-family: Arial; max-width: 860px; margin: 20px auto; background:#f8fafc; }}
      .card {{ background:white; padding:16px; border-radius:10px; margin-bottom:12px; box-shadow:0 1px 4px rgba(0,0,0,.08); }}
      button {{ padding:8px 12px; margin:3px; }}
      input {{ padding:8px; }}
    </style></head>
    <body>
      <div class='card'><h2>🤖 Media Auto-Forward Dashboard</h2><p>Status: <b>{state}</b></p></div>
      <div class='card'>
        <h3>📊 Live Stats</h3>
        <p>📦 Total: {stats['total']} | ⏳ Pending: {stats['pending']} | ✅ Sent: {stats['sent']} | ❌ Failed: {stats['failed']}</p>
        <p>👥 Admins: {stats['admins']} | 💾 Size: {stats['size']} bytes</p>
      </div>
      <div class='card'>
        <h3>⚙️ Settings</h3>
        <p>Delay: {runtime.delay_seconds}s | Max Size: {runtime.max_file_size_mb}MB | Prefix: {prefix}</p>
        <form method='post' action='/toggle{token_suffix}'><button name='enabled' value='1'>✅ Enable</button><button name='enabled' value='0'>⏸️ Disable</button></form>
        <form method='post' action='/delay{token_suffix}'>⏱️ Delay(s): <input name='value' /><button type='submit'>Update</button></form>
        <form method='post' action='/prefix{token_suffix}'>📝 Prefix: <input name='value' /><button type='submit'>Update</button></form>
      </div>
      <div class='card'>
        <h3>🧰 Queue Actions</h3>
        <form method='post' action='/retryfailed{token_suffix}'><button type='submit'>🔁 Retry Failed</button></form>
        <form method='post' action='/clearqueue{token_suffix}'><button type='submit'>🗑️ Clear Queue</button></form>
      </div>
    </body></html>
    """


async def maybe_start_web_panel(db: Database, settings: Settings):
    if not settings.web_panel_enabled:
        return None
    try:
        from fastapi import FastAPI, Form, Request
        from fastapi.responses import HTMLResponse, PlainTextResponse
        import uvicorn
    except Exception:
        logger.warning("WEB_PANEL_ENABLED=true but fastapi/uvicorn not installed.")
        return None

    app = FastAPI(title="Media Auto Forward Panel")

    def authorized(request: Request) -> bool:
        if not settings.web_panel_token:
            return True
        return request.query_params.get("t") == settings.web_panel_token

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        async with db.session() as session:
            repo = Repository(session)
            stats = await repo.stats()
            runtime = await repo.get_runtime_settings()
        token_suffix = f"?t={settings.web_panel_token}" if settings.web_panel_token else ""
        return HTMLResponse(build_dashboard_html(stats, runtime, token_suffix))

    @app.post("/toggle")
    async def toggle(request: Request, enabled: int = Form(...)):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()
            runtime.is_enabled = enabled == 1
            await session.commit()
        return PlainTextResponse("OK")

    @app.post("/delay")
    async def set_delay(request: Request, value: str = Form(...)):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        try:
            delay = float(value)
            if delay < 0:
                raise ValueError
        except ValueError:
            return PlainTextResponse("Invalid", status_code=400)
        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()
            runtime.delay_seconds = delay
            await session.commit()
        return PlainTextResponse("OK")

    @app.post("/prefix")
    async def set_prefix(request: Request, value: str = Form("")):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        async with db.session() as session:
            repo = Repository(session)
            runtime = await repo.get_runtime_settings()
            runtime.caption_prefix = value or None
            await session.commit()
        return PlainTextResponse("OK")

    @app.post("/retryfailed")
    async def retry_failed(request: Request):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        async with db.session() as session:
            await Repository(session).retry_failed()
        return PlainTextResponse("OK")

    @app.post("/clearqueue")
    async def clear_queue(request: Request):
        if not authorized(request):
            return PlainTextResponse("Unauthorized", status_code=401)
        async with db.session() as session:
            await Repository(session).clear_queue()
        return PlainTextResponse("OK")

    config = uvicorn.Config(app, host=settings.web_panel_host, port=settings.web_panel_port, log_level="info")
    server = uvicorn.Server(config)
    return asyncio.create_task(server.serve())


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    db = Database(settings)
    await db.create_schema()
    async with db.session() as session:
        repo = Repository(session)
        await repo.ensure_runtime_settings()
        await repo.bootstrap_admins(settings.admin_ids)

    user_client_kwargs = {
        "name": settings.user_session_name,
        "api_id": settings.api_id,
        "api_hash": settings.api_hash,
    }
    if settings.user_session_string:
        user_client_kwargs["session_string"] = settings.user_session_string

    user_client = Client(
        name=settings.user_session_name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        session_string="BAJHOo4AlfEfSi4h_dg1JOeTunZyFQjorXUnNx-ZFdhWhS30WLUr0XHDjBJN58cnQ75L8DKoHj4wPcgzIWoEj9HHaIq74Yef8eIRwVn2vQO9OCJxi1HkLCYHIYXepCHEqIrhHdp1oX9732AAnkM-6NtJXOUE3H1N3ZdlBFCLRkzdBpEcn9oISY4PT5eoFDszQqbinCN6lnfUkqH0-usvE0elN2WnXCBWorJ9zUPVL5fffrBszCOb4Z0-cJK3ro4Njh2vulJ8VS5OkuOGpbo6Xoq002cOYfNDhNiHU_Hmhf5XZYJAFSIsUO9n_5w8oAzzt84F0wk7huldQEtihQ0DUrIDQRFoXAAAAAGxwi8lAA",  # required in container
    )

    bot_client = Client(
        name=settings.bot_session_name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        bot_token=settings.bot_token,
    )

    listener = SavedMessagesListener(user_client, db, settings)
    listener.register_handlers()
    register_admin_handlers(bot_client, db, settings)
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
    panel_task = await maybe_start_web_panel(db, settings)
    logger.info("medi_save_auto_forward started")

    try:
        await stop_event.wait()
    finally:
        await worker.stop()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

        if panel_task:
            panel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await panel_task

        await bot_client.stop()
        await user_client.stop()
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(run())
