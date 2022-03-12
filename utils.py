import sys
import sqlite3
import csv
from contextlib import contextmanager
from datetime import datetime
from config import settings


def save_csv(path, columns, rows):
    with open(path, mode="w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


class Db:
    def __init__(self, path=settings.sqlite_path) -> None:
        self.path = path

    @contextmanager
    def connect(self):
        cursor = sqlite3.connect(self.path)
        try:
            yield cursor
        except sqlite3.ProgrammingError as err:
            error, = err.args
            sys.stderr.write(error.message)
        finally:
            cursor.commit()
            cursor.close()

    def init_objects(self):
        with self.connect() as db:
            sql = '''
                CREATE TABLE IF NOT EXISTS jobs (
                    row_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    modified_at         TIMESTAMP,
                    task_id             VARCHAR(32),
                    keyword             VARCHAR(30),
                    country             VARCHAR(20),
                    job_id              INT,
                    company             VARCHAR(150),
                    title               VARCHAR(250),
                    location            VARCHAR(100),
                    salary              VARCHAR(40),
                    description         TEXT,
                    skills_frameworks   TEXT,
                    skills_databases    TEXT,
                    skills_platform     TEXT,
                    skills_prog_langs   TEXT
                );

                CREATE INDEX IF NOT EXISTS IX_jobs_job_id ON jobs (job_id);
            '''
            db.executescript(sql)

    def add_jobs(self, data: list):
        sql = """INSERT INTO jobs (task_id, keyword, country, job_id, company, title, salary)
        VALUES(?,?,?,?,?,?,?)
        """
        with self.connect() as db:
            db.executemany(sql, data)

    def update_job(self, company, location, description, row_id):
        sql = "UPDATE jobs SET modified_at = ?, company = ?, location = ?, description = ? WHERE row_id = ?;"
        with self.connect() as db:
            db.execute(sql, (datetime.now(), company, location, description, row_id))

    def delete_job(self, row_id):
        sql = "DELETE FROM jobs WHERE row_id = ?"
        with self.connect() as db:
            db.execute(sql, (row_id,))

    def get_not_ready_jobs(self):
        # Remove duplicate rows by post_id if exists
        sql = """
        ;WITH RemoveDuplicates AS (
            SELECT row_id
            ,   CASE WHEN ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY row_id) > 1 THEN 1 ELSE 0 END AS duplicated
            FROM jobs
        )
        DELETE FROM jobs
        WHERE row_id IN (SELECT row_id FROM RemoveDuplicates WHERE duplicated = 1);
        """
        with self.connect() as db:
            db.execute(sql)

        sql = "SELECT row_id, job_id FROM jobs WHERE description IS NULL ORDER BY row_id;"
        with self.connect() as db:
            return db.execute(sql).fetchall()
    
    def get_job_description(self, row_id) -> str:
        with self.connect() as db:
            sql = 'SELECT description FROM jobs WHERE row_id = ?;'
            return db.execute(sql, (row_id,)).fetchone()[0]

    def update_skilss(self, data):
        with self.connect() as db:
            sql = """
            UPDATE jobs SET
                skills_prog_langs   = ?,
                skills_platform     = ?,
                skills_databases    = ?,
                skills_frameworks   = ?
            WHERE row_id = ?
            """
            db.execute(sql, data)

    def waiting_for_extract(self) -> list:
        with self.connect() as db:
            sql = 'SELECT row_id FROM jobs ORDER BY row_id;'
            return [int(x[0]) for x in db.execute(sql).fetchall()]
