import sqlite3
import os
from typing import List
import logging

from common import ensure_path
import gitparse
from gitstat_models import *

LOGGER_TAG = gitparse.GIT_PARSE_LOGGER_ID
log = logging.getLogger(LOGGER_TAG)

class GitStatCache:
	def __init__(self, config:GitStatConfig, db_file_name:str="git_stat_cache.db"):
		self.config = config
		self.db_file_name = db_file_name
		self.cwd = os.getcwd()
		self.create_db()

	@property
	def db_path(self):
		return os.path.join(self.config.cache_path, self.db_file_name)

	def create_db(self):
		ensure_path(self.config.cache_path)
		log.info(f"Using cache db file: '{self.db_path}'")
		self.db = sqlite3.connect(self.db_path)
		self.db_cursor = self.db.cursor()
		self.db_cursor.execute("CREATE TABLE IF NOT EXISTS commit_cache(id INTEGER PRIMARY KEY, tag TEXT, repo TEXT, commit_timestamp INTEGER, insertions INTEGER, deletions INTEGER, commit_hash TEXT, UNIQUE(repo, commit_hash))")
		self.db_cursor.execute('''CREATE TABLE IF NOT EXISTS repo_mapping (
                id INTEGER PRIMARY KEY,
                repo_id INTEGER UNIQUE,
                repo_name TEXT,
                default_branch TEXT,
                url TEXT UNIQUE,
                stars INTEGER,
                forks INTEGER,
                size INTEGER,
                tag TEXT,
                is_cloned BOOL,
                failed INTEGER,
                last_commit_hash TEXT,
                UNIQUE(repo_name, last_commit_hash)
            )''')

	def load_meta(self, repo_meta:RepoMeta) -> RepoMeta:
		cached_repo_meta = self.db_cursor.execute(f"SELECT * FROM repo_mapping WHERE repo_id={repo_meta.id}").fetchone()
		if cached_repo_meta:
			return RepoMeta(db_row=cached_repo_meta)
		return repo_meta

	def load_metas(self, repo_mapping:RepoMapping) -> List[RepoMeta]:
		for row in self.db_cursor.execute(f"SELECT * FROM repo_mapping WHERE tag=\"{repo_mapping.tag}\"").fetchall():
			yield RepoMeta(db_row=row)

	def update_meta(self, repo_meta:RepoMeta):
		cached = self.db_cursor.execute(f"SELECT * FROM repo_mapping WHERE repo_id={repo_meta.id}").fetchone()
		if cached:
			self.db_cursor.execute(f"UPDATE repo_mapping SET last_commit_hash=\"{repo_meta.last_commit_hash}\" WHERE repo_id={repo_meta.id}")
		else:
			last_commit_hash_str = "NULL"
			if repo_meta.last_commit_hash:
				last_commit_hash_str = f"\"{repo_meta.last_commit_hash}\""
			sql = "INSERT INTO repo_mapping (repo_id, repo_name, default_branch, url, stars, forks, size, tag, is_cloned, failed, last_commit_hash) VALUES " + f"({repo_meta.id}, \"{repo_meta.repo_name}\", \"{repo_meta.default_branch}\", \"{repo_meta.url}\",  {repo_meta.stars}, {repo_meta.forks}, {repo_meta.size}, \"{repo_meta.tag}\", {repo_meta.is_cloned}, {repo_meta.failed}, {last_commit_hash_str})"
			self.db_cursor.execute(sql)

	def update_commits(self, repo_meta:RepoMeta) -> str:
		last_commit_hash = None
		os.chdir(self.cwd)
		for commit in gitparse.get_commits(self.config.repo_directory(repo_meta), last_hash=repo_meta.last_commit_hash, start_date=self.config.max_history_time):

			if not last_commit_hash:
				last_commit_hash = commit.commit_hash
			try:
				self.db_cursor.execute("INSERT INTO commit_cache (tag, repo, commit_timestamp, insertions, deletions, commit_hash) VALUES (?, ?, ?, ?, ?, ?)",
					(repo_meta.tag, repo_meta.repo_name, commit.date, commit.insertions, commit.deletions, commit.commit_hash))
			except sqlite3.IntegrityError as e:
				log.error(f"IntegrityError for {repo_meta.tag} {repo_meta.repo_name}. Hash: {commit.commit_hash}")
				raise e
		if last_commit_hash and repo_meta.last_commit_hash != last_commit_hash:
			repo_meta.last_commit_hash = last_commit_hash
			self.update_meta(repo_meta)
		self.db.commit()
		os.chdir(self.cwd)
		return last_commit_hash

	def get_commits(self, repo_meta:RepoMeta) -> List[gitparse.CommitData]:
		self.db_cursor.execute(f"SELECT * FROM commit_cache WHERE tag=\"{repo_meta.tag}\" AND repo=\"{repo_meta.repo_name}\"")
		for row in self.db_cursor.fetchall():
			yield gitparse.CommitData(db_row=row)