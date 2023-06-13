from bs4 import BeautifulSoup
import contextlib
import datetime
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
from urllib.parse import urljoin

system = platform.system()

with open("config.json") as f:
    c = f.read()
    CONFIG = json.loads(c)

logger.remove(0)
logger.add(sys.stderr, level="INFO")


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

    def insert(self, cols, rows, tbname: str, index_label: str):
        inserted = []
        with contextlib.closing(
            sqlite3.connect(self.db_name)
        ) as con, con, contextlib.closing(con.cursor()) as cur:
            md5_index = cols.index("md5")
            for row in rows:
                md5 = row[md5_index]
                query = f"SELECT COUNT(*) FROM {tbname} WHERE {index_label} = ?"
                cur.execute(query, (md5,))
                result = cur.fetchone()[0]
                if result > 0:
                    continue
                else:
                    cur.execute(
                        f"INSERT INTO {tbname} VALUES({','.join(['?' for i in range(len(cols))])})",
                        row,
                    )
                    inserted.append(row)
        return inserted


class LineWorker:
    def __init__(self) -> None:
        self.admin_id = CONFIG.get("line_admin_id")
        self.group_chat_id = CONFIG.get("line_group_chat_id")
        self.api_client = LineBotApi(CONFIG.get("line_channel_access_token"))
        self.handler_client = WebhookHandler(CONFIG.get("line_channel_secret"))

    def get_id(self, to: str):
        if to == "admin":
            return self.admin_id
        elif to == "group_chat":
            return self.group_chat_id

    def send_text_message(self, to: str, text: str):
        id_ = self.get_id(to=to)
        self.api_client.push_message(id_, TextSendMessage(text))

    def send_flex_message(self, to: str, alt_text: str, flex_message):
        id_ = self.get_id(to=to)
        self.api_client.push_message(
            id_, FlexSendMessage(alt_text=alt_text, contents=flex_message)
        )


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


class ZSJHSWorker(Worker):
    def __init__(self, db: DB, line_worker: LineWorker) -> None:
        super().__init__()
        self.name: str = "中山國中"
        self.base_url: str = "http://www.csjhs.tp.edu.tw/news/"
        self.zsjhs_url: str = "u_news_v1.asp?id={F246F2F4-4F1E-42DA-B518-5FB731FD672F}"
        self.db: DB = db
        self.line_worker: LineWorker = line_worker
        self.message_title = f"羽球場-{self.name}"
        with open("line_flex_message_template.json", "r", encoding="utf-8") as f:
            self.message_template = json.load(f)

    def format_message(self, columns, row):
        template = self.message_template
        template["body"]["contents"][0]["text"] = self.message_title
        template["body"]["contents"][1]["contents"][1]["text"] = row[
            columns.index("標題")
        ]
        template["body"]["contents"][2]["contents"][1]["action"][
            "uri"
        ] = self.get_shorten_url(row[columns.index("標題連結")])
        template["body"]["contents"][3]["contents"][1]["text"] = row[
            columns.index("發布日期")
        ]
        return template

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

    def insert_to_db(self, cols, rows):
        return self.db.insert(cols, rows, "zsjhs", "md5")

    def main(self):
        page_content = self.get_content(urljoin(self.base_url, self.zsjhs_url))
        cols, rows = self.combine_post_and_content(page_content)
        inserted = self.insert_to_db(cols, rows)
        if not inserted:
            logger.info(f"沒有找到{self.name}相關的通知。")
        for row in inserted:
            self.line_worker.send_flex_message(
                to="group_chat",
                alt_text=f"羽球場地通知-{self.name}",
                flex_message=self.format_message(cols, row),
            )
            logger.info(
                f'發送訊息: 標題: {row[cols.index("標題")]}, 發布日期: {row[cols.index("發布日期")]}'
            )


class YHESWorker(Worker):
    def __init__(self, db: DB, line_worker: LineWorker) -> None:
        super().__init__()
        self.name: str = "玉成國小"
        self.cols: list = ["title", "link", "published", "description", "md5"]
        self.rss_url: str = "https://www.yhes.tp.edu.tw/nss/main/feeder/5a9759adef37531ea27bf1b0/Cq0o5XU2162?f=normal&vector=private&static=false"
        self.db: DB = db
        self.line_worker: LineWorker = line_worker
        self.message_title = f"羽球場-{self.name}"
        self.filter_pattern = r"羽球|場地|租借"
        self.published_format_string_in = "%a, %d %b %Y %H:%M:%S %Z"
        if system == "Windows":
            self.published_format_string_out = "%Y/%#m/%#d"
        else:
            self.published_format_string_out = "%Y/%-m/%-d"
        with open("line_flex_message_template.json", "r", encoding="utf-8") as f:
            self.message_template = json.load(f)

    def get_rss_data(self):
        return feedparser.parse(self.rss_url)

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

    def insert_to_db(self, cols, rows):
        return self.db.insert(cols, rows, "yhes", "md5")

    def format_message(self, columns, row):
        template = self.message_template
        template["body"]["contents"][0]["text"] = self.message_title
        template["body"]["contents"][1]["contents"][1]["text"] = row[
            columns.index("title")
        ]
        template["body"]["contents"][2]["contents"][1]["action"][
            "uri"
        ] = self.get_shorten_url(row[columns.index("link")])
        template["body"]["contents"][3]["contents"][1]["text"] = row[
            columns.index("published")
        ]
        return template

    def main(self):
        d = self.get_rss_data()
        rss_data = self.extract_rss_data(d)
        rss_data = self.filter_rss_data(rss_data)
        inserted = self.insert_to_db(self.cols, rss_data)
        if not inserted:
            logger.info(f"沒有找到{self.name}相關的通知。")
        for row in inserted:
            self.line_worker.send_flex_message(
                to="group_chat",
                alt_text=f"羽球場地通知-{self.name}",
                flex_message=self.format_message(self.cols, row),
            )
            logger.info(
                f'發送訊息: 標題: {row[self.cols.index("title")]}, 發布日期: {row[self.cols.index("published")]}'
            )


if __name__ == "__main__":

    def run_all(workers: list):
        for worker in workers:
            try:
                logger.info(
                    f"Run Scheduled Job: {datetime.datetime.now(pytz.timezone(CONFIG.get('tz')))} with {worker.name}"
                )
                worker.main()
            except Exception as e:
                logger.exception(e)
                line_worker.send_text_message(
                    to="admin", text=str(traceback.format_exc())
                )

    db = DB()
    line_worker = LineWorker()
    zsjhs_worker = ZSJHSWorker(db, line_worker)
    yhes_worker = YHESWorker(db, line_worker)
    schedule.every(CONFIG.get("default_schedule_job_interval")).seconds.do(
        lambda: run_all([zsjhs_worker, yhes_worker])
    )

    logger.info(
        f"Job Started At: {datetime.datetime.now(pytz.timezone(CONFIG.get('tz')))}"
    )
    while True:
        schedule.run_pending()
        time.sleep(1)
