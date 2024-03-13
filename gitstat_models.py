from typing import List, Dict
import requests
import os
import gitparse

class RepoMapping:
	def __init__(self, org:str, tag:str=None, max_count:int=0, ignore_repos: List[str] = []):
		self.org = org
		self.max_count = max_count
		self.ignore_repos = ignore_repos
		if not tag:
			self.tag = org
		else:
			self.tag = tag

class RepoMeta:
	def __init__(self, repo_id:int=0, default_branch:str=None, url:str=None, name:str=None, stars:int=0, watchers:int=0, forks:int=0, size:int=0, tag:str=None, is_cloned:bool=False, last_commit_hash:str=None, db_row=None):
		
		if db_row:
			self.update(db_row)
		else:
			self.id = repo_id
			self.default_branch = default_branch
			self.url = url
			self.repo_name = name
			self.stars = stars
			self.watchers = watchers
			self.forks = forks
			self.size = size
			self.tag = tag
			self.is_cloned = is_cloned
			self.failed = 0
			self.last_commit_hash = last_commit_hash

	@property
	def as_dict(self):
		return {
			"id"  : self.id,
			"default_branch" : self.default_branch,
			"url" : self.url,
			"name" : self.repo_name,
			"stars" : self.stars, 
			"watchers" : self.watchers,
			"forks" : self.forks,
			"size" : self.size,
			"tag" : self.tag,
			"is_cloned" : self.is_cloned,
			"failed" : self.failed,
			"last_commit_hash" : self.last_commit_hash
		}

	def update(self, db_row):
		_, self.id, self.repo_name, self.default_branch, self.url, self.stars, self.forks, self.size, self.tag, self.is_cloned, self.failed, self.last_commit_hash = db_row

	@property
	def name(self):
		return f"git_repo_{self.tag}_{self.id}"

	def __str__(self):
		return f"{self.as_dict})"

	def __repr__(self):
		return self.__str__()

class GitStatEntry:

	def __init__(self, timestamp:int, period_interval:int, change_count:int, commit_count:int, insertions:int, deletions:int):
		self.timestamp = timestamp
		self.period_interval = period_interval
		self.change_count = change_count
		self.commit_count = commit_count
		self.insertions = insertions
		self.deletions = deletions

	def add(self, commit:gitparse.CommitData):
		self.change_count = self.change_count + commit.insertions + commit.deletions
		self.commit_count = self.commit_count + 1
		self.insertions = self.insertions + commit.insertions
		self.deletions = self.deletions + commit.deletions

	@property
	def as_dict(self):
		return {
			"commit_count": self.commit_count,
			"timestamp": self.timestamp,
			"change_count": self.change_count,
			"insertions": self.insertions,
			"deletions": self.deletions
		}

	def __str__(self):
		return f"{self.as_dict})"

	def __repr__(self):
		return self.__str__()

# Represent a list of `GitStatPeriodEntry`
class GitStatRepository:

	def __init__(self, entries:List[GitStatEntry], repo_meta:RepoMeta, period_interval:int):
		self.entries = entries
		self.period_interval = period_interval
		self.repo_meta = repo_meta

	@property
	def name(self):
		return f"git_stat_period_{self.repo_meta.name}_{self.period_interval}"

class GitStatConfig:
	def __init__(self, repository_path:str, cache_path:str = "./cache", max_history_time:int=0, include_forks:bool=False, repos_per_page:int=100, base_url:str="https://api.github.com", network=requests):
		self.repository_path = repository_path
		self.cache_path = cache_path
		self.max_history_time = max_history_time
		self.repos_per_page = repos_per_page
		self.base_url = base_url
		self.network = network
		self.include_forks = include_forks

	def tag_directory(self, tag:str):
		return os.path.join(self.repository_path, tag)

	def repo_directory(self, repo_meta:RepoMeta):
		return os.path.join(self.tag_directory(repo_meta.tag), repo_meta.repo_name)