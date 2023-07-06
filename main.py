import asyncio
from bs4 import BeautifulSoup
import contextlib
import datetime
import discord
import feedparser
import json
import hashlib
from linebot import LineBotApi, WebhookHandler
from linebot.models import TextSendMessage, FlexSendMessage
from loguru import logger
import pytz
import platform
import requests
import re
import schedule
import sys
import sqlite3
import time
import traceback
from typing import Union, Literal, Optional
from urllib.parse import urljoin

system = platform.system()

with open("config.json") as f:
    c = f.read()
    CONFIG = json.loads(c)

logger.remove(0)
logger.add(sys.stderr, level="INFO")


### database worker ###
class DB:
    def __init__(self) -> None:
        self.db_name = "bcdb.db"
        self.init_db()

    def init_db(self):
        # 中山國中
        q = """CREATE TABLE IF NOT EXISTS zsjhs
            (
                序號 VARCHAR(10),
                標題 VARCHAR(100),
                標題連結 VARCHAR(100),
                發布單位 VARCHAR(20),
                發布日期 VARCHAR(10),
                詳細內容 TEXT,
                相關連結 VARCHAR(10),
                相關檔案 VARCHAR(10),
                md5 VARCHAR(10) PRIMARY KEY
            );
        """
        self.execute(q)
        # 玉成國小
        q = """CREATE TABLE IF NOT EXISTS yhes
            (
                title VARCHAR(100),
                link VARCHAR(100),
                published VARCHAR(20),
                description TEXT,
                md5 VARCHAR(10) PRIMARY KEY
            );
        """
        self.execute(q)
        # 三民國中
        q = """CREATE TABLE IF NOT EXISTS smjh
            (
                title VARCHAR(100),
                link VARCHAR(100),
                published VARCHAR(20),
                description TEXT,
                md5 VARCHAR(10) PRIMARY KEY
            );
        """
        self.execute(q)

    def execute(self, sql: str):
        with contextlib.closing(
            sqlite3.connect(self.db_name)
        ) as con, con, contextlib.closing(con.cursor()) as cur:
            cur.execute(sql)

    def query(self, sql: str):
        with contextlib.closing(
            sqlite3.connect(self.db_name)
        ) as con, con, contextlib.closing(con.cursor()) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def exist(self, cols, row, tbname: str, index_label: str):
        with contextlib.closing(
            sqlite3.connect(self.db_name)
        ) as con, con, contextlib.closing(con.cursor()) as cur:
            md5_index = cols.index("md5")
            md5 = row[md5_index]
            query = f"SELECT COUNT(*) FROM {tbname} WHERE {index_label} = ?"
            cur.execute(query, (md5,))
            result = cur.fetchone()[0]
            return result > 0

    def insert(self, cols, row, tbname: str):
        with contextlib.closing(
            sqlite3.connect(self.db_name)
        ) as con, con, contextlib.closing(con.cursor()) as cur:
            cur.execute(
                f"INSERT INTO {tbname} VALUES({','.join(['?' for i in range(len(cols))])})",
                row,
            )


### message workers ###
class LineWorker:
    def __init__(self) -> None:
        self.admin_id = CONFIG.get("line_admin_id")
        self.group_chat_id = CONFIG.get("line_group_chat_id")
        self.channel_access_token = CONFIG.get("line_channel_access_token")
        self.handler_client = WebhookHandler(CONFIG.get("line_channel_secret"))
        with open("line_flex_message_template.json", "r", encoding="utf-8") as f:
            self.flex_message_template = json.load(f)

    def get_id(self, to: str):
        if to == "admin":
            return self.admin_id
        elif to == "group_chat":
            return self.group_chat_id

    def format_flex_message(
        self,
        message_title: str,
        title_color: str,
        title: str,
        link: str,
        published: str,
    ):
        template = self.flex_message_template
        template["body"]["contents"][0]["text"] = message_title
        template["body"]["contents"][0]["color"] = title_color
        template["body"]["contents"][1]["contents"][1]["text"] = title
        template["body"]["contents"][2]["contents"][1]["action"]["uri"] = link
        template["body"]["contents"][3]["contents"][1]["text"] = published
        return template

    def send_text_message(self, to: str, text: str):
        id_ = self.get_id(to=to)
        api_client = LineBotApi(self.channel_access_token)
        api_client.push_message(id_, TextSendMessage(text))

    def send_flex_message(self, to: str, alt_text: str, flex_message):
        id_ = self.get_id(to=to)
        api_client = LineBotApi(self.channel_access_token)
        api_client.push_message(
            id_, FlexSendMessage(alt_text=alt_text, contents=flex_message)
        )


class DiscordWorker:
    def __init__(self) -> None:
        self.token: str = CONFIG.get("discord_token")
        self.admin_channel_id: int = 1126352290492723322
        self.channel_id: int = 1125437819607863307

    def format_message(
        self,
        title: str,
        message_title: str,
        link: str,
        published: str,
    ):
        return f"**{title}**\n標題: {message_title}\n連結: {link}\n日期: {published}"

    async def send_message(self, to: str, text: str = ""):
        client = discord.Client(intents=discord.Intents.default())
        if to == "admin":
            channel_id = self.admin_channel_id
        elif to == "normal":
            channel_id = self.channel_id

        @client.event
        async def on_ready():
            nonlocal channel_id
            nonlocal text

            channel = client.get_channel(channel_id)

            if channel:
                # Send the message to the channel
                await channel.send(text)
                logger.info("Message sent successfully.")
            else:
                logger.error("Invalid channel ID.")

            # Close the client connection
            await client.close()

        # Run the client asynchronously
        await client.start(self.token)


### base worker ###
class Worker:
    def __init__(self) -> None:
        self.reurl_post_uri = CONFIG.get("reurl_post_uri")
        self.reurl_api_key = CONFIG.get("reurl_api_key")

    def get_content(self, url: str):
        r = requests.get(url)
        return r.content.decode("utf-8")

    def get_shorten_url(self, url: str):
        try:
            r = requests.post(
                url=self.reurl_post_uri,
                headers={
                    "Content-Type": "application/json",
                    "reurl-api-key": self.reurl_api_key,
                },
                data='{"url":"' + url + '"}',
            )
            return r.json()["short_url"]
        except Exception as e:
            logger.error(e)
            return ""

    def hash_row_data(self, row):
        md5 = hashlib.md5()
        for data in row:
            md5.update(data.encode("utf-8"))
        return md5.hexdigest()


### workers ###
class ZSJHSWorker(Worker):
    def __init__(
        self, db: DB, message_worker: Union[LineWorker, DiscordWorker]
    ) -> None:
        super().__init__()
        self.name: str = "中山國中"
        self.db: DB = db
        self.message_worker: Union[LineWorker, DiscordWorker] = message_worker
        self.table_name: str = "zsjhs"
        self.title_color: str = "#f5a142"
        self.base_url: str = "http://www.csjhs.tp.edu.tw/news/"
        self.zsjhs_url: str = "u_news_v1.asp?id={F246F2F4-4F1E-42DA-B518-5FB731FD672F}"
        self.message_title = f"羽球場-{self.name}"

    def send_message(self, col, row):
        if isinstance(self.message_worker, LineWorker):
            self.message_worker.send_flex_message(
                to="group_chat",
                alt_text=f"羽球場地通知-{self.name}",
                flex_message=self.message_worker.format_flex_message(
                    message_title=self.message_title,
                    title_color=self.title_color,
                    title=row[col.index("標題")],
                    link=self.get_shorten_url(row[col.index("標題連結")]),
                    published=row[col.index("發布日期")],
                ),
            )
        elif isinstance(self.message_worker, DiscordWorker):
            asyncio.run(
                self.message_worker.send_message(
                    to="normal",
                    text=self.message_worker.format_message(
                        title=self.message_title,
                        message_title=row[col.index("標題")],
                        link=self.get_shorten_url(row[col.index("標題連結")]),
                        published=row[col.index("發布日期")],
                    ),
                )
            )

    def extract_columns(self, soup: BeautifulSoup, q: str, to_exclude: list):
        table = soup.find("table", {"summary": re.compile(q)})
        columns = [
            re.sub(r"\s+", "", th.text) for th in table.find("tr").find_all("th")
        ]
        columns = list(filter(lambda x: x not in to_exclude, columns))
        return columns

    def extract_posts(self, page_content: str):
        soup = BeautifulSoup(page_content, "html.parser")
        columns = self.extract_columns(soup, "場地租借", ["點閱次數"])
        trs = soup.find_all("tr", {"class": re.compile("C-tableA2|C-tableA3")})
        rows = []
        for tr in trs:
            row = []
            for idx, td in enumerate(tr.find_all("td")):
                a_tag = td.find("a")
                if a_tag:
                    if f"{columns[idx]}連結" not in columns:
                        columns.insert(idx + 1, f"{columns[idx]}連結")
                    row.append(re.sub(r"\s+", "", td.text))
                    row.append(urljoin(self.base_url, a_tag.get("href")))
                elif idx == 4:
                    continue
                else:
                    row.append(re.sub(r"\s+", "", td.text))
            rows.append(row)
        return columns, rows

    def extract_post_content(self, post_content_url: str):
        post_content = self.get_content(post_content_url)
        soup = BeautifulSoup(post_content, "html.parser")
        columns = self.extract_columns(soup, "\*", ["點閱次數", "標題", "發布日期", "發布單位"])
        rows = [
            re.sub(
                r"\s+",
                "",
                soup.find("th", string=re.compile(c)).find_next_sibling("td").text,
            )
            for c in columns
        ]
        return columns, rows

    def combine_post_and_content(self, page_content):
        cols, rows = self.extract_posts(page_content)
        url_idx = cols.index("標題連結")

        combine_cols = True
        for row in rows:
            ccols, crows = self.extract_post_content(row[url_idx])
            if combine_cols:
                cols += ccols
                combine_cols = False
            row += crows
        cols, rows = self.adding_md5_value(cols, rows)
        return cols, rows

    def adding_md5_value(self, columns: list, rows: list[list]):
        if "md5" not in columns:
            columns.append("md5")
        for row in rows:
            md5 = self.hash_row_data(row)
            row.append(md5)
        return columns, rows

    def insert_to_db(self, cols, row):
        return self.db.insert(cols, row, self.table_name)

    def main(self):
        try:
            page_content = self.get_content(urljoin(self.base_url, self.zsjhs_url))
            cols, rows = self.combine_post_and_content(page_content)
            count = 0
            for row in rows:
                if not self.db.exist(cols, row, self.table_name, "md5"):
                    count += 1
                    self.send_message(cols, row)
                    self.insert_to_db(cols, row)
                    logger.info(
                        f'發送訊息: 標題: {row[cols.index("標題")]}, 發布日期: {row[cols.index("發布日期")]}'
                    )
            if count == 0:
                logger.info(f"沒有找到{self.name}相關的通知。")
        except Exception as e:
            logger.exception(e)
            raise


class RSSWorker(Worker):
    def __init__(self) -> None:
        super().__init__()
        self.filter_pattern = r"羽球|場地|租借"
        self.cols: list = ["title", "link", "published", "description", "md5"]
        self.published_format_string_in = "%a, %d %b %Y %H:%M:%S %Z"
        if system == "Windows":
            self.published_format_string_out = "%Y/%#m/%#d"
        else:
            self.published_format_string_out = "%Y/%-m/%-d"

    def get_rss_data(self, url: str):
        return feedparser.parse(url)

    def extract_rss_data(self, d):
        rss_data = []
        for entry in d["entries"]:
            title = entry.get("title", "")
            link = entry.get("link", "")
            published_str = entry.get("published", "")
            published = (
                datetime.datetime.strptime(
                    published_str, self.published_format_string_in
                ).strftime(self.published_format_string_out)
                if published_str != ""
                else ""
            )
            description = re.sub(r"<.*?>|\s+", "", entry.get("description", ""))
            md5 = self.hash_row_data([title, link, published, description])
            rss_data.append([title, link, published, description, md5])
        return rss_data

    def filter_rss_data(self, rss_data: list):
        return list(filter(lambda x: re.search(self.filter_pattern, x[0]), rss_data))


class YHESWorker(RSSWorker):
    def __init__(
        self, db: DB, message_worker: Union[LineWorker, DiscordWorker]
    ) -> None:
        super().__init__()
        self.name: str = "玉成國小"
        self.db: DB = db
        self.message_worker: Union[LineWorker, DiscordWorker] = message_worker
        self.table_name: str = "yhes"
        self.title_color: str = "#51f542"
        self.rss_url: str = "https://www.yhes.tp.edu.tw/nss/main/feeder/5a9759adef37531ea27bf1b0/Cq0o5XU2162?f=normal&vector=private&static=false"
        self.message_title = f"羽球場-{self.name}"
        self.filter_pattern = r"羽球|場地|租借"

    def insert_to_db(self, cols, row):
        return self.db.insert(cols, row, self.table_name)

    def send_message(self, col, row):
        if isinstance(self.message_worker, LineWorker):
            self.message_worker.send_flex_message(
                to="group_chat",
                alt_text=f"羽球場地通知-{self.name}",
                flex_message=self.message_worker.format_flex_message(
                    message_title=self.message_title,
                    title_color=self.title_color,
                    title=row[col.index("title")],
                    link=self.get_shorten_url(row[col.index("link")]),
                    published=row[col.index("published")],
                ),
            )
        elif isinstance(self.message_worker, DiscordWorker):
            asyncio.run(
                self.message_worker.send_message(
                    to="normal",
                    text=self.message_worker.format_message(
                        title=self.message_title,
                        message_title=row[col.index("title")],
                        link=self.get_shorten_url(row[col.index("link")]),
                        published=row[col.index("published")],
                    ),
                )
            )

    def main(self):
        try:
            d = self.get_rss_data(self.rss_url)
            rss_data = self.extract_rss_data(d)
            rss_data = self.filter_rss_data(rss_data)
            count = 0
            for row in rss_data:
                if not self.db.exist(self.cols, row, self.table_name, "md5"):
                    count += 1
                    self.send_message(self.cols, row)
                    self.insert_to_db(self.cols, row)
                    logger.info(
                        f'發送訊息: 標題: {row[self.cols.index("title")]}, 發布日期: {row[self.cols.index("published")]}'
                    )
            if count == 0:
                logger.info(f"沒有找到{self.name}相關的通知。")
        except Exception as e:
            logger.exception(e)
            raise


class SMJHWorker(RSSWorker):
    def __init__(self, db: DB, message_worker: LineWorker) -> None:
        super().__init__()
        self.name: str = "三民國中"
        self.db: DB = db
        self.message_worker: Union[LineWorker, DiscordWorker] = message_worker
        self.table_name: str = "smjh"
        self.title_color: str = "#4287f5"
        self.rss_url: str = "https://www.smjh.tp.edu.tw/nss/main/feeder/5abf2d62aa93092cee58ceb4/P6nJedk3190?f=normal&%240=KJQUup08386&vector=private&static=false"
        self.message_title = f"羽球場-{self.name}"
        self.filter_pattern = r"羽球|場地|租借"

    def insert_to_db(self, cols, row):
        return self.db.insert(cols, row, self.table_name)

    def send_message(self, col, row):
        if isinstance(self.message_worker, LineWorker):
            self.message_worker.send_flex_message(
                to="group_chat",
                alt_text=f"羽球場地通知-{self.name}",
                flex_message=self.message_worker.format_flex_message(
                    message_title=self.message_title,
                    title_color=self.title_color,
                    title=row[col.index("title")],
                    link=self.get_shorten_url(row[col.index("link")]),
                    published=row[col.index("published")],
                ),
            )
        elif isinstance(self.message_worker, DiscordWorker):
            asyncio.run(
                self.message_worker.send_message(
                    to="normal",
                    text=self.message_worker.format_message(
                        title=self.message_title,
                        message_title=row[col.index("title")],
                        link=self.get_shorten_url(row[col.index("link")]),
                        published=row[col.index("published")],
                    ),
                )
            )

    def main(self):
        try:
            d = self.get_rss_data(self.rss_url)
            rss_data = self.extract_rss_data(d)
            rss_data = self.filter_rss_data(rss_data)
            count = 0
            for row in rss_data:
                if not self.db.exist(self.cols, row, self.table_name, "md5"):
                    count += 1
                    self.send_message(self.cols, row)
                    self.insert_to_db(self.cols, row)
                    logger.info(
                        f'發送訊息: 標題: {row[self.cols.index("title")]}, 發布日期: {row[self.cols.index("published")]}'
                    )
            if count == 0:
                logger.info(f"沒有找到{self.name}相關的通知。")
        except Exception as e:
            logger.exception(e)
            raise


class BCFinder:
    def __init__(
        self,
        db: DB,
        message_worker: Union[LineWorker, DiscordWorker],
        workers: list[Literal["中山國中", "玉成國小", "三民國中"]],
    ) -> None:
        self.db = db()
        self.message_worker = message_worker()
        self.worker_type: dict = {
            "中山國中": ZSJHSWorker,
            "玉成國小": YHESWorker,
            "三民國中": SMJHWorker,
        }
        assert len(workers) > 0, f"請至少註冊一種worker: {self.worker_type.keys()}"
        assert (
            len(
                _invalid_workers := [
                    worker for worker in workers if worker not in self.worker_type
                ]
            )
        ) == 0, f"註冊的worker無法辨識: {_invalid_workers}"
        self.workers: list[
            Union[ZSJHSWorker, YHESWorker, SMJHWorker]
        ] = self.create_workers(self.db, self.message_worker, workers)

    def create_workers(
        self, db: DB, message_worker: Union[LineWorker, DiscordWorker], workers: list
    ):
        return [self.worker_type[worker](db, message_worker) for worker in workers]

    def send_message(self, text: str):
        if isinstance(self.message_worker, LineWorker):
            self.message_worker.send_text_message(to="admin", text=text)
        elif isinstance(self.message_worker, DiscordWorker):
            asyncio.run(self.message_worker.send_message(to="admin", text=text))

    def run_all(self):
        for worker in self.workers:
            try:
                logger.info(
                    f"Run Scheduled Job: {datetime.datetime.now(pytz.timezone(CONFIG.get('tz')))} with {worker.name}"
                )
                worker.main()
            except Exception as e:
                logger.exception(e)
                self.send_message(text=str(traceback.format_exc()))


if __name__ == "__main__":
    bcfinder = BCFinder(
        db=DB, message_worker=DiscordWorker, workers=["中山國中", "玉成國小", "三民國中"]
    )
    schedule.every(CONFIG.get("default_schedule_job_interval")).seconds.do(
        bcfinder.run_all
    )

    logger.info(
        f"Job Started At: {datetime.datetime.now(pytz.timezone(CONFIG.get('tz')))}"
    )
    while True:
        schedule.run_pending()
        time.sleep(1)
