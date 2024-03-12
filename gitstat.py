import subprocess
import requests
import time
import os
import pytz
from datetime import datetime
from typing import List, Dict
import logging

from common import ensure_path
import gitparse

LOGGER_TAG = gitparse.GIT_PARSE_LOGGER_ID
log = logging.getLogger(LOGGER_TAG)

class GitStatConfig:
	def __init__(self, repository_path:str, max_history_time:int=0, cache_path:str="./cache", repos_per_page:int=100, base_url:str="https://api.github.com", network=requests):
		self.repository_path = repository_path
		self.max_history_time = max_history_time
		self.repos_per_page = repos_per_page
		self.base_url = base_url
		self.cache_path = cache_path
		self.network = network

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
	def __init__(self, id:str, default_branch:str, url:str, name:str, stars:int, watchers:int, forks:int, size:int, tag:str, is_cloned:bool=False, last_commit_hash:str=None):
		self.id = id
		self.default_branch = default_branch
		self.url = url
		self.repo_name = name
		self.stars = stars
		self.watchers = watchers
		self.forks = forks,
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
			"timestamp": self.timestamp,
			"change_count": self.change_count,
			"commit_count": self.commit_count,
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

class GitStats:

	def __init__(self, config:GitStatConfig):
		self.config = config
		self.cwd = os.getcwd()

	# Fetch repository metadata from github using the contents of repo_mappings_container ([{"tag": String, "org": String }])
	def fetch_repositories_meta(self, repo_mappings: List[RepoMapping]) -> Dict[str, List[RepoMeta]]:
		
		metadata = dict()

		for repo_mapping in repo_mappings:
			print(repo_mapping.tag)
			metadata[repo_mapping.tag] = self._fetch_repository_meta(repo_mapping)
			time.sleep(1)
		
		return metadata


	def _fetch_repository_meta(self, repo_mapping:RepoMapping, page:int=0) -> List[RepoMeta]:

		repos_url = self.repo_url(repo_mapping, page)
		if not repos_url:
			return []
		log.info(f"Fetchg repositories for {repo_mapping.tag} : {repos_url}")
		response = self.config.network.get(repos_url)
		if(response.status_code != 200):
			log.error(f"ERROR: Invalid response for {repos_url}: {response.status_code}")
			return []

		repo_metas = []
		repo_count = 0
		for repo in response.json():

			repo_count = repo_count + 1

			if repo["name"] in repo_mapping.ignore_repos:
				continue
			repo_meta = RepoMeta(repo["id"], repo["default_branch"], repo["clone_url"], repo["name"], repo["stargazers_count"], repo["watchers_count"], repo["forks"], repo["size"], repo_mapping.tag)
			repo_metas.append(repo_meta)

		if repo_count == self.config.repos_per_page:
			time.sleep(1)
			repo_metas = repo_metas + self._fetch_repository_meta(repo_mapping, page + 1)
		
		return repo_metas

	def repo_url(self, repo_mapping:RepoMapping, page:int=0):
		path = f"orgs/{repo_mapping.org}/repos"
		repo_count = self.config.repos_per_page
		if repo_mapping.max_count:
			if repo_count + self.config.repos_per_page * page > repo_mapping.max_count:
				repo_count = repo_mapping.max_count - self.config.repos_per_page * page
		if repo_count <= 0:
			return None
		return f"{self.config.base_url}/{path}?sort=updated&direction=desc&type=public&per_page={repo_count}&page={page}"	

	def _tag_directory(self, tag:str):
		return os.path.join(self.config.repository_path, tag)

	def _repo_directory(self, repo_meta:RepoMeta):
		return os.path.join(self._tag_directory(repo_meta.tag), repo_meta.repo_name)

	def download_source_code(self, repos_metas:Dict[str, List[RepoMeta]]) -> Dict[str, List[RepoMeta]]:

		for tag in repos_metas.keys():
			os.chdir(self.cwd)
			repo_metas = repos_metas[tag]
			base_dir = self._tag_directory(tag)
			ensure_path(base_dir)
			for (i, repo_meta) in enumerate(repo_metas):
				repo_dir = self._repo_directory(repo_meta)

				if repo_meta.is_cloned or (os.path.isdir(repo_dir) and os.path.isdir(os.path.join(repo_dir, ".git"))):
					log.info(f"Updating {repo_meta.repo_name} for {repo_meta.tag} ({repo_meta.url})")
					subprocess.Popen(["git", "checkout", "."], cwd=repo_dir)
					process = subprocess.Popen(["git", "pull"], stdout=subprocess.PIPE, cwd=repo_dir)
					result, _ = process.communicate()
					if not process.returncode == 0:
						log.error("Failed to update:", repo_dir, "Message:", result.decode("utf8", 'ignore'))
						repos_metas[tag][i].failed = repos_metas[tag][i].failed + 1
					else:
						repos_metas[tag][i].is_cloned = True
						repos_metas[tag][i].failed = 0 
				else:
					ensure_path(repo_dir)
					log.info(f"Cloning {repo_meta.repo_name} for {repo_meta.tag} ({repo_meta.url})")
					process = subprocess.Popen(["git", "clone", repo_meta.url, repo_meta.repo_name], stdout=subprocess.PIPE, cwd=base_dir)
					result, _ = process.communicate()
					if not process.returncode == 0:
						repos_metas[tag][i].failed = repos_metas[tag][i].failed + 1
					else:
						repos_metas[tag][i].cloned = True
						repos_metas[tag][i].failed = 0
			os.chdir(self.cwd)
		return repos_metas

	def _get_raw_commits(self, repo_meta:RepoMeta) -> List[gitparse.CommitData]:
		return gitparse.get_commits(self._repo_directory(repo_meta), last_hash=repo_meta.last_commit_hash, start_date=self.config.max_history_time)

	def calculate_timestamp(self, commit_timestamp:int, period_interval:int) -> int:
		periods = int(commit_timestamp / period_interval) + 1
		return periods * period_interval

	# Generate stats for a single repository
	def generate_repository_stats(self, period_interval:int, repo_meta:RepoMeta=None, commits:List[gitparse.CommitData]=None) -> GitStatRepository:
		stats = dict()
		current = None
		if not commits:
			assert(repo_meta)
			os.chdir(self.cwd)
			commits = self._get_raw_commits(repo_meta)
			os.chdir(self.cwd)
		for commit in commits:
			if commit.date < self.config.max_history_time:
				continue
			timestamp = self.calculate_timestamp(commit.date, period_interval)
			if timestamp in stats:
				current = stats[timestamp]
				current.add(commit)
			else:
				current = GitStatEntry(timestamp=timestamp, 
								  period_interval=period_interval, 
								  change_count=commit.insertions+commit.deletions, 
								  commit_count=1, 
								  insertions=commit.insertions, 
								  deletions=commit.deletions)
		
			stats[timestamp] = current
		
		return GitStatRepository(stats.values(), repo_meta, period_interval)

	# Generate stats for a set of repositories
	def generate_stats(self, period_interval:int, repo_metas:List[RepoMeta]) -> [GitStatEntry]:
		stats = dict()
		current = None
		for repo_meta in repo_metas:

			repository_stats = self.generate_repository_stats(period_interval, repo_meta)
			for entry in repository_stats.entries:
				if entry.timestamp in stats:
					current = stats[entry.timestamp]
					current.add(entry)
				else:
					current = entry

				stats[entry.timestamp] = current

		return stats.values()