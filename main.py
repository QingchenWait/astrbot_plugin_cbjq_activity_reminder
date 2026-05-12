import asyncio
import html
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star


LIST_API = (
    "https://www.cbjq.com/api.php?op=search_api&action=get_article_list"
    "&catid=7131&page=1&num=16&order_by=inputtime"
)
DETAIL_API = (
    "https://www.cbjq.com/api.php?op=search_api&action=get_article_detail"
    "&catid={catid}&id={article_id}"
)
DEFAULT_CONFIG: Dict[str, Any] = {
    "query_prefix": "【尘白活动日历】",
    "reminder_prefix": "【尘白活动提醒】",
    "underground_reference_date": "",
    "underground_cycle_days": 14,
    "auto_reminder_enabled": False,
    "auto_reminder_time": "20:00",
    "reminder_sessions": [],
    "cache_ttl_minutes": 60,
    "ongoing_end_within_days": 14,
    "upcoming_start_within_days": 14,
    "allow_at_query": True,
    "request_timeout_seconds": 15,
    "news_list_api": LIST_API,
}
EVENT_TITLE_RE = re.compile(
    r"(?:^|[。；;!！?\n\r])\s*(?:[一二三四五六七八九十百]+[、.．]\s*)?"
    r"(?:✧\s*)?(?P<title>(?:【[^】]{2,80}】|「[^」]{2,80}」)[^\n\r。；;：:]{0,40})"
)
TIME_RANGE_RE = re.compile(
    r"(?P<label>活动时间|开放时间|上架时间)"
    r"[:：]\s*(?P<range>[^。\n\r；;]{8,90})"
)
DATE_POINT_RE = re.compile(
    r"(?:(?P<year>\d{4})年)?(?P<month>\d{1,2})月(?P<day>\d{1,2})日"
    r"(?:(?:\s*\([^)]*\))|(?:\s*（[^）]*）))*"
    r"\s*(?P<suffix>维护后|停机更新维护|更新后|开服后|"
    r"(?P<hour>\d{1,2})[:：](?P<minute>\d{1,2}))?"
)
INVALID_ACTIVITY_KEYWORDS = (
    "共鸣",
    "特选共鸣",
    "标准共鸣",
    "角色定向",
    "武器定向",
    "武器",
    "时装",
    "外观",
    "限时签到",
    "签到活动",
    "限时补充",
    "凭证",
    "特别物资补给",
    "物资补给",
    "补给限时",
    "家具上新",
    "限时折扣",
    "体验主线",
    "主线特别篇",
    "供应站",
)


@dataclass(frozen=True)
class Activity:
    name: str
    version_name: str
    start: datetime
    end: datetime
    source_title: str
    source_url: str

    @property
    def identity(self) -> str:
        return "|".join(
            [
                self.name,
                self.start.strftime("%Y-%m-%d %H:%M"),
                self.end.strftime("%Y-%m-%d %H:%M"),
            ]
        )


@dataclass(frozen=True)
class ScheduleData:
    version_name: str
    source_title: str
    source_url: str
    fetched_at: datetime
    activities: Tuple[Activity, ...]


@dataclass(frozen=True)
class UndergroundStatus:
    start: date
    end: date
    days_left: int
    cycle_days: int


@dataclass(frozen=True)
class ReminderItem:
    identity: str
    name: str
    end_at: datetime
    end_text: str


class Main(Star):
    """尘白禁区国服活动日历查询与自动提醒。"""

    def __init__(self, context: Context, config: Optional[Dict[str, Any]] = None):
        super().__init__(context)
        self.config = config or {}
        self._cache: Optional[ScheduleData] = None
        self._cache_at: Optional[datetime] = None
        self._reminder_task: Optional[asyncio.Task] = None
        self._reminder_wakeup: Optional[asyncio.Event] = None
        self._reminded_key = "reminded_activity_days"

    async def initialize(self) -> None:
        self._wrap_config_save()
        await self._clear_runtime_cache("插件启动、热重载或服务重启")
        if self._reminder_task is None or self._reminder_task.done():
            self._reminder_wakeup = asyncio.Event()
            self._reminder_task = asyncio.create_task(self._reminder_loop())

    async def terminate(self) -> None:
        if self._reminder_task:
            self._reminder_task.cancel()
            try:
                await self._reminder_task
            except asyncio.CancelledError:
                pass
            self._reminder_task = None
        self._reminder_wakeup = None

    @filter.command("cbjq", alias={"尘白活动", "尘白日历"})
    async def query_schedule(self, event: AstrMessageEvent):
        """查询尘白禁区当期活动日历"""
        message = await self._build_query_response()
        yield self._stop_result(event, message)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def mention_query_schedule(self, event: AstrMessageEvent):
        """允许管理员配置 @bot 尘白活动 查询"""
        if not self._cfg_bool("allow_at_query"):
            return
        if not self._is_at_bot(event):
            return
        if not self._is_query_text(event.message_str):
            return

        event.stop_event()
        message = await self._build_query_response()
        yield self._stop_result(event, message)

    @filter.command("cbjq_refresh", alias={"尘白刷新"})
    async def refresh_schedule(self, event: AstrMessageEvent):
        """刷新尘白禁区活动缓存"""
        try:
            await self._clear_runtime_cache("手动刷新")
            schedule = await self._get_schedule(force=True)
            message = (
                f"{self._cfg('query_prefix')}\n"
                f"已刷新：{schedule.version_name}，共识别 {len(schedule.activities)} 个活动。"
            )
        except Exception as exc:
            logger.warning("尘白活动刷新失败: %s", exc)
            message = f"{self._cfg('query_prefix')}\n刷新失败：{exc}"
        yield self._stop_result(event, message)

    @filter.command("cbjq_subscribe", alias={"尘白订阅提醒"})
    async def subscribe_reminder(self, event: AstrMessageEvent):
        """订阅当前会话的尘白活动自动提醒"""
        if not await self._can_manage_current_session(event):
            yield self._stop_result(
                event,
                f"{self._cfg('query_prefix')}\n只有 AstrBot 管理员或当前 QQ 群群主可以订阅自动提醒。",
            )
            return

        sessions = self._get_reminder_sessions()
        if event.unified_msg_origin not in sessions:
            sessions.append(event.unified_msg_origin)
            self.config["reminder_sessions"] = sessions
            self._save_config()
        yield self._stop_result(
            event,
            f"{self._cfg('query_prefix')}\n已订阅当前会话的自动提醒。",
        )

    @filter.command("cbjq_unsubscribe", alias={"尘白取消提醒"})
    async def unsubscribe_reminder(self, event: AstrMessageEvent):
        """取消当前会话的尘白活动自动提醒"""
        if not await self._can_manage_current_session(event):
            yield self._stop_result(
                event,
                f"{self._cfg('query_prefix')}\n只有 AstrBot 管理员或当前 QQ 群群主可以取消自动提醒。",
            )
            return

        sessions = self._get_reminder_sessions()
        if event.unified_msg_origin in sessions:
            sessions.remove(event.unified_msg_origin)
            self.config["reminder_sessions"] = sessions
            self._save_config()
        yield self._stop_result(
            event,
            f"{self._cfg('query_prefix')}\n已取消当前会话的自动提醒。",
        )

    async def _reminder_loop(self) -> None:
        while True:
            try:
                if await self._sleep_until_next_check():
                    await self._send_due_reminders()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("尘白活动自动提醒任务异常: %s", exc)
                await asyncio.sleep(60)

    async def _sleep_until_next_check(self) -> bool:
        target = self._parse_clock_time(str(self._cfg("auto_reminder_time")))
        now = datetime.now()
        next_run = datetime.combine(now.date(), target)
        if next_run <= now:
            next_run += timedelta(days=1)
        timeout = max(1.0, (next_run - now).total_seconds())
        wakeup = self._reminder_wakeup
        if wakeup is None:
            await asyncio.sleep(timeout)
            return True
        try:
            await asyncio.wait_for(wakeup.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return True
        wakeup.clear()
        return False

    async def _send_due_reminders(self) -> None:
        if not self._cfg_bool("auto_reminder_enabled"):
            return
        sessions = self._get_reminder_sessions()
        if not sessions:
            logger.info("尘白自动提醒已启用，但 reminder_sessions 为空。")
            return

        now = datetime.now()
        reminder_items = await self._collect_due_reminder_items(now)
        if not reminder_items:
            logger.info("尘白自动提醒检查完成：暂无 1 天内结束的活动或地下清理。")
            return

        today_key = now.strftime("%Y-%m-%d")
        reminded = await self.get_kv_data(self._reminded_key, {})
        if not isinstance(reminded, dict):
            reminded = {}
        sent_ids = set(reminded.get(today_key, []))
        pending = [item for item in reminder_items if item.identity not in sent_ids]
        if not pending:
            return

        message = self._format_reminder_message(pending, now)
        chain = MessageChain().message(message)
        for session in sessions:
            try:
                await self.context.send_message(session, chain)
            except Exception as exc:
                logger.warning("向 %s 发送尘白活动提醒失败: %s", session, exc)

        reminded[today_key] = sorted(
            sent_ids | {item.identity for item in pending}
        )
        for old_key in list(reminded.keys()):
            if old_key < (now.date() - timedelta(days=14)).strftime("%Y-%m-%d"):
                reminded.pop(old_key, None)
        await self.put_kv_data(self._reminded_key, reminded)

    async def _collect_due_reminder_items(self, now: datetime) -> List[ReminderItem]:
        items: List[ReminderItem] = []
        try:
            schedule = await self._get_schedule()
        except Exception as exc:
            logger.warning("获取尘白活动日历失败，跳过活动过期提醒: %s", exc)
        else:
            items.extend(
                self._activity_reminder_item(activity)
                for activity in schedule.activities
                if now < activity.end <= now + timedelta(days=1)
            )

        underground = self._get_underground_status(now.date())
        if underground and underground.days_left == 1:
            items.append(self._underground_reminder_item(underground))
        return items

    async def _get_schedule(self, force: bool = False) -> ScheduleData:
        now = datetime.now()
        ttl = max(0, self._cfg_int("cache_ttl_minutes"))
        if (
            not force
            and self._cache is not None
            and self._cache_at is not None
            and ttl > 0
            and now - self._cache_at < timedelta(minutes=ttl)
        ):
            return self._cache

        target_article = await self._fetch_latest_activity_article()
        if not target_article:
            raise ValueError("未在新闻列表中找到“限时活动公告”。")

        catid = str(target_article.get("catid") or "7131")
        article_id = str(target_article.get("id") or "")
        detail_url = DETAIL_API.format(catid=catid, article_id=article_id)
        detail_data = await self._fetch_json(detail_url)
        detail_items = detail_data.get("data", [])
        if not isinstance(detail_items, list) or not detail_items:
            raise ValueError("官网详情 API 返回格式异常。")

        detail = detail_items[0]
        schedule = self._parse_schedule(detail, target_article)
        self._cache = schedule
        self._cache_at = now
        return schedule

    async def _fetch_json(self, url: str) -> Dict[str, Any]:
        timeout = aiohttp.ClientTimeout(
            total=max(3, self._cfg_int("request_timeout_seconds"))
        )
        headers = {
            "User-Agent": "AstrBot-CBJQ-Activity-Reminder/v1.0.3",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.cbjq.com/",
        }
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                text = await response.text()
                payload = json.loads(text.lstrip("\ufeff"))
                if not isinstance(payload, dict):
                    raise ValueError("接口返回不是 JSON object。")
                return payload

    async def _fetch_latest_activity_article(self) -> Optional[Dict[str, Any]]:
        list_url = str(self._cfg("news_list_api"))
        for page in range(1, 4):
            page_url = self._replace_page_param(list_url, page)
            list_data = await self._fetch_json(page_url)
            articles = list_data.get("data", {}).get("list", [])
            if not isinstance(articles, list):
                raise ValueError("官网列表 API 返回格式异常。")
            target = self._select_activity_article(articles)
            if target:
                return target
            if not articles:
                break
        return None

    def _select_activity_article(
        self,
        articles: Sequence[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for article in articles:
            title = str(article.get("title") or "")
            if "限时活动公告" in title and "《尘白禁区》" in title:
                return article
        for article in articles:
            title = str(article.get("title") or "")
            if "限时活动公告" in title:
                return article
        return None

    @staticmethod
    def _replace_page_param(url: str, page: int) -> str:
        if re.search(r"([?&]page=)\d+", url):
            return re.sub(r"([?&]page=)\d+", lambda m: f"{m.group(1)}{page}", url)
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}page={page}"

    def _parse_schedule(
        self,
        detail: Dict[str, Any],
        list_article: Dict[str, Any],
    ) -> ScheduleData:
        title = str(detail.get("title") or list_article.get("title") or "")
        version_name = self._extract_version_name(title)
        source_url = str(detail.get("url") or list_article.get("url") or "")
        publish_time = self._parse_publish_time(
            detail.get("inputtime") or list_article.get("inputtime")
        )
        text = self._html_to_text(str(detail.get("content") or ""))
        activities = self._extract_activities(
            text=text,
            version_name=version_name,
            source_title=title,
            source_url=source_url,
            publish_time=publish_time,
        )
        if not activities:
            raise ValueError("已找到活动公告，但没有识别到有效活动时间。")
        return ScheduleData(
            version_name=version_name,
            source_title=title,
            source_url=source_url,
            fetched_at=datetime.now(),
            activities=tuple(activities),
        )

    def _extract_activities(
        self,
        text: str,
        version_name: str,
        source_title: str,
        source_url: str,
        publish_time: datetime,
    ) -> List[Activity]:
        matches = list(TIME_RANGE_RE.finditer(text))
        activities: List[Activity] = []
        seen = set()
        for match in matches:
            raw_range = match.group("range")
            parsed = self._parse_time_range(raw_range, publish_time)
            if not parsed:
                continue
            start, end = parsed
            name = self._find_activity_name(text, match.start(), version_name)
            if self._should_skip_activity(name):
                continue
            identity = (name, start, end)
            if identity in seen:
                continue
            seen.add(identity)
            activities.append(
                Activity(
                    name=name,
                    version_name=version_name,
                    start=start,
                    end=end,
                    source_title=source_title,
                    source_url=source_url,
                )
            )
        activities.sort(key=lambda item: (item.start, item.end, item.name))
        return activities

    def _find_activity_name(self, text: str, time_pos: int, version_name: str) -> str:
        window = text[max(0, time_pos - 260) : time_pos]
        for raw_line in reversed([line.strip() for line in window.splitlines()]):
            line = self._normalize_activity_name(raw_line)
            if not self._looks_like_activity_title(line):
                continue
            return line

        candidates = list(EVENT_TITLE_RE.finditer(window))
        if candidates:
            title = candidates[-1].group("title")
            return self._normalize_activity_name(title)

        line_start = max(window.rfind("\n"), window.rfind("。"), window.rfind("；"))
        if line_start >= 0:
            candidate = window[line_start + 1 :].strip()
            candidate = re.sub(
                r"^(?:✧\s*)?(?:[一二三四五六七八九十百]+[、.．]\s*)?",
                "",
                candidate,
            )
            candidate = candidate.replace("活动时间", "").strip(" ：:")
            if 2 <= len(candidate) <= 40:
                return self._normalize_activity_name(candidate)

        return f"{version_name}限时活动"

    @staticmethod
    def _looks_like_activity_title(line: str) -> bool:
        if not (2 <= len(line) <= 80):
            return False
        if any(
            token in line
            for token in (
                "活动时间",
            "活动内容",
            "参与条件",
            "开放购买时间",
            "购买时间",
            "领取时间",
            "兑换时间",
            "小提示",
            "特别提醒",
            "内容包括",
            )
        ):
            return False
        return any(token in line for token in ("【", "「", "限时", "玩法", "活动"))

    def _parse_time_range(
        self,
        raw_range: str,
        publish_time: datetime,
    ) -> Optional[Tuple[datetime, datetime]]:
        clean = self._normalize_text(raw_range)
        clean = clean.replace("~", " - ").replace("至", " - ").replace("—", " - ")
        points = list(DATE_POINT_RE.finditer(clean))
        if len(points) < 2:
            return None
        start = self._date_match_to_datetime(points[0], publish_time, is_end=False)
        end = self._date_match_to_datetime(points[1], publish_time, is_end=True)
        if start is None or end is None:
            return None
        if start > end:
            end = end.replace(year=end.year + 1)
        return start, end

    def _date_match_to_datetime(
        self,
        match: re.Match,
        publish_time: datetime,
        is_end: bool,
    ) -> Optional[datetime]:
        year_text = match.group("year")
        year = int(year_text) if year_text else publish_time.year
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour_text = match.group("hour")
        minute_text = match.group("minute")
        suffix = match.group("suffix") or ""
        if hour_text is not None and minute_text is not None:
            hour = int(hour_text)
            minute = int(minute_text)
        elif suffix in ("维护后", "停机更新维护", "更新后", "开服后"):
            hour = 4
            minute = 0
        else:
            hour = 4
            minute = 0
        try:
            value = datetime(year, month, day, hour, minute)
        except ValueError:
            return None

        if not year_text and publish_time - value > timedelta(days=90):
            try:
                value = value.replace(year=value.year + 1)
            except ValueError:
                return None
        return value

    def _format_query_message(self, schedule: ScheduleData, now: datetime) -> str:
        ongoing = [
            activity
            for activity in schedule.activities
            if activity.start <= now < activity.end
            and activity.end <= now + timedelta(days=self._cfg_positive_int("ongoing_end_within_days", 14))
        ]
        upcoming = [
            activity
            for activity in schedule.activities
            if activity.start > now
            and activity.start <= now + timedelta(days=self._cfg_positive_int("upcoming_start_within_days", 14))
        ]
        ongoing.sort(key=lambda item: (item.end, item.name))
        upcoming.sort(key=lambda item: (item.start, item.name))

        lines = [
            str(self._cfg("query_prefix")),
            f"版本：{schedule.version_name}",
            "",
            "正在进行的活动：",
        ]
        if ongoing:
            for activity in ongoing:
                days = self._days_until(activity.end, now)
                lines.append(
                    f"- {activity.name}：{self._fmt_date(activity.end)} 结束，剩余 {days} 天"
                )
        else:
            lines.append("- 暂无")

        lines.extend(["", "即将开始的活动："])
        if upcoming:
            for activity in upcoming:
                days = self._days_until(activity.start, now)
                lines.append(
                    f"- {activity.name}：{self._fmt_date(activity.start)} 开始，{days} 天后开始"
                )
        else:
            lines.append("- 暂无")

        underground = self._get_underground_status(now.date())
        lines.extend(["", "地下清理："])
        if underground:
            lines.append(
                f"- 当期 {underground.start:%Y-%m-%d} 至 {underground.end:%Y-%m-%d}，"
                f"距离更新还剩 {underground.days_left} 天"
            )
        else:
            lines.append("- 未配置开始日期，请管理员在 WebUI 配置。")
        return "\n".join(lines)

    def _format_reminder_message(
        self,
        items: Iterable[ReminderItem],
        now: datetime,
    ) -> str:
        lines = [
            str(self._cfg("reminder_prefix")),
            "以下内容将在 1 天内结束：",
        ]
        for item in sorted(items, key=lambda value: (value.end_at, value.name)):
            lines.append(f"- {item.name}：{item.end_text} 结束")
        return "\n".join(lines)

    def _activity_reminder_item(self, activity: Activity) -> ReminderItem:
        return ReminderItem(
            identity=activity.identity,
            name=activity.name,
            end_at=activity.end,
            end_text=self._fmt_date(activity.end),
        )

    @staticmethod
    def _underground_reminder_item(underground: UndergroundStatus) -> ReminderItem:
        return ReminderItem(
            identity=f"underground|{underground.start.isoformat()}|{underground.end.isoformat()}",
            name="地下清理",
            end_at=datetime.combine(underground.end, time.min),
            end_text=f"{underground.end:%m-%d}",
        )

    def _get_underground_status(self, today: date) -> Optional[UndergroundStatus]:
        ref = self._parse_date(str(self._cfg("underground_reference_date")))
        if not ref:
            return None
        cycle_days = max(1, self._cfg_int("underground_cycle_days"))
        delta = (today - ref).days
        cycles = math.floor(delta / cycle_days)
        current_start = ref + timedelta(days=cycles * cycle_days)
        if current_start > today:
            current_start -= timedelta(days=cycle_days)
        current_end = current_start + timedelta(days=cycle_days)
        days_left = max(0, (current_end - today).days)
        return UndergroundStatus(
            start=current_start,
            end=current_end,
            days_left=days_left,
            cycle_days=cycle_days,
        )

    def _get_reminder_sessions(self) -> List[str]:
        raw = self._cfg("reminder_sessions")
        if not isinstance(raw, list):
            return []
        return [str(item).strip() for item in raw if str(item).strip()]

    def _save_config(self) -> None:
        save_config = getattr(self.config, "save_config", None)
        if callable(save_config):
            save_config()

    def _wrap_config_save(self) -> None:
        original_save = getattr(self.config, "_cbjq_original_save_config", None)
        save_config = original_save or getattr(self.config, "save_config", None)
        if not callable(save_config):
            return

        def wrapped_save_config(*args, **kwargs):
            result = save_config(*args, **kwargs)
            self._on_config_saved()
            return result

        try:
            object.__setattr__(self.config, "_cbjq_original_save_config", save_config)
            object.__setattr__(self.config, "save_config", wrapped_save_config)
        except Exception as exc:
            logger.warning("尘白插件无法挂接配置保存事件，将仅在插件重载时清理缓存: %s", exc)

    def _on_config_saved(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._clear_memory_cache()
            return
        loop.create_task(self._clear_runtime_cache("配置已保存"))

    async def _clear_runtime_cache(self, reason: str) -> None:
        self._clear_memory_cache()
        try:
            await self.delete_kv_data(self._reminded_key)
        except Exception as exc:
            logger.warning("清理尘白提醒记录缓存失败: %s", exc)
        self._wake_reminder_loop()
        logger.info("已清理尘白插件运行缓存：%s。", reason)

    def _clear_memory_cache(self) -> None:
        self._cache = None
        self._cache_at = None

    def _wake_reminder_loop(self) -> None:
        if self._reminder_wakeup is not None:
            self._reminder_wakeup.set()

    @staticmethod
    def _stop_result(event: AstrMessageEvent, message: str):
        event.stop_event()
        return event.plain_result(message).stop_event()

    async def _can_manage_current_session(self, event: AstrMessageEvent) -> bool:
        if event.is_admin():
            return True
        if not event.get_group_id():
            return False
        try:
            group = await event.get_group()
        except Exception as exc:
            logger.warning("获取群信息失败，无法判断尘白订阅权限: %s", exc)
            return False
        if not group or not group.group_owner:
            return False
        return str(event.get_sender_id()) == str(group.group_owner)

    def _cfg(self, key: str) -> Any:
        value = self.config.get(key) if hasattr(self.config, "get") else None
        if value is None and key in DEFAULT_CONFIG:
            return DEFAULT_CONFIG[key]
        return value

    def _cfg_int(self, key: str) -> int:
        try:
            return int(self._cfg(key))
        except (TypeError, ValueError):
            return int(DEFAULT_CONFIG[key])

    def _cfg_positive_int(self, key: str, fallback: int) -> int:
        try:
            value = int(self._cfg(key))
        except (TypeError, ValueError):
            value = fallback
        return max(1, value)

    def _cfg_bool(self, key: str) -> bool:
        value = self._cfg(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on", "是", "开")
        return bool(value)

    async def _build_query_response(self) -> str:
        try:
            schedule = await self._get_schedule()
            return self._format_query_message(schedule, datetime.now())
        except Exception as exc:
            logger.warning("尘白活动查询失败: %s", exc)
            return f"{self._cfg('query_prefix')}\n获取活动日历失败：{exc}"

    def _is_at_bot(self, event: AstrMessageEvent) -> bool:
        self_id = str(event.get_self_id() or "")
        for comp in event.get_messages():
            if comp.__class__.__name__ != "At":
                continue
            target = str(getattr(comp, "qq", "") or "")
            if self_id and target == self_id:
                return True
        return False

    @staticmethod
    def _is_query_text(text: str) -> bool:
        clean = re.sub(r"^@\S+\s*", "", text.strip())
        clean = clean.lstrip("/").strip()
        return clean in ("cbjq", "尘白活动", "尘白日历")

    @staticmethod
    def _extract_version_name(title: str) -> str:
        match = re.search(r"[「【](.*?)[」】]", title)
        if match:
            return match.group(1).strip()
        return "当期版本"

    @staticmethod
    def _parse_publish_time(value: Any) -> datetime:
        try:
            timestamp = int(str(value))
        except (TypeError, ValueError):
            return datetime.now()
        if timestamp > 10_000_000_000:
            timestamp = int(timestamp / 1000)
        return datetime.fromtimestamp(timestamp)

    @staticmethod
    def _parse_date(value: str) -> Optional[date]:
        value = value.strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_clock_time(value: str) -> time:
        value = value.strip()
        match = re.match(r"^(\d{1,2})[:：](\d{1,2})$", value)
        if not match:
            return time(20, 0)
        hour = min(23, max(0, int(match.group(1))))
        minute = min(59, max(0, int(match.group(2))))
        return time(hour, minute)

    @staticmethod
    def _html_to_text(raw_html: str) -> str:
        text = re.sub(r"(?i)<br\s*/?>", "\n", raw_html)
        text = re.sub(r"(?i)</p\s*>", "\n", text)
        text = re.sub(r"(?i)</div\s*>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        return Main._normalize_text(html.unescape(text))

    @staticmethod
    def _normalize_text(text: str) -> str:
        text = html.unescape(text)
        text = text.replace("\u00a0", " ")
        text = text.replace("\r", "\n")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n\s+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _normalize_activity_name(name: str) -> str:
        name = Main._normalize_text(name)
        name = re.sub(
            r"^(?:✧\s*)?(?:[一二三四五六七八九十百]+[、.．]\s*)?",
            "",
            name,
        )
        name = name.strip(" ：:。；;，,")
        name = name.replace("·", "-")
        return name

    @staticmethod
    def _should_skip_activity(name: str) -> bool:
        compact = re.sub(r"\s+", "", name)
        return any(keyword in compact for keyword in INVALID_ACTIVITY_KEYWORDS)

    @staticmethod
    def _days_until(target: datetime, now: datetime) -> int:
        seconds = (target - now).total_seconds()
        if seconds <= 0:
            return 0
        return max(1, math.ceil(seconds / 86400))

    @staticmethod
    def _fmt_period(activity: Activity) -> str:
        return f"{activity.start:%m-%d} 至 {activity.end:%m-%d}"

    @staticmethod
    def _fmt_date(value: datetime) -> str:
        return f"{value:%m-%d}"
