import subprocess
import requests
import time
import os
import pytz
from datetime import datetime
from typing import List, Dict
import logging


from common import ensure_path
from gitparse_cache import GitStatCache
import gitparse
from gitstat_models import *


LOGGER_TAG = gitparse.GIT_PARSE_LOGGER_ID
log = logging.getLogger(LOGGER_TAG)

class GitStats:

	def __init__(self, config:GitStatConfig, cache:GitStatCache=None):
		self.config = config
		
		self.cache = cache
		if not cache:
			self.cache = GitStatCache(config)

		self.cwd = os.getcwd()

	# Fetch repository metadata from github using the contents of repo_mappings_container ([{"tag": String, "org": String }])
	def fetch_repositories_meta(self, repo_mappings: List[RepoMapping]) -> Dict[str, List[RepoMeta]]:
		
		metadata = dict()

		for repo_mapping in repo_mappings:
			metadata[repo_mapping.tag] = self._fetch_repository_meta(repo_mapping)
			time.sleep(1)
		
		return metadata

	def _fetch_repository_meta(self, repo_mapping:RepoMapping, page:int=0) -> List[RepoMeta]:

		repos_url = self._repo_urls(repo_mapping, page)
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
			if not self.config.include_forks and repo["fork"]:
				continue
			repo_meta = RepoMeta(repo["id"], repo["default_branch"], repo["clone_url"], repo["name"], repo["stargazers_count"], repo["watchers_count"], repo["forks_count"], repo["size"], repo_mapping.tag)
			repo_metas.append(repo_meta)

		if repo_count == self.config.repos_per_page:
			time.sleep(1)
			repo_metas = repo_metas + self._fetch_repository_meta(repo_mapping, page + 1)
		
		return repo_metas

	def _repo_urls(self, repo_mapping:RepoMapping, page:int=0):
		path = f"orgs/{repo_mapping.org}/repos"
		repo_count = self.config.repos_per_page
		if repo_mapping.max_count:
			if repo_count + self.config.repos_per_page * page > repo_mapping.max_count:
				repo_count = repo_mapping.max_count - self.config.repos_per_page * page
		if repo_count <= 0:
			return None
		return f"{self.config.base_url}/{path}?sort=updated&direction=desc&type=public&per_page={repo_count}&page={page}"

	def download_source_code(self, repos_metas:Dict[str, List[RepoMeta]]) -> Dict[str, List[RepoMeta]]:

		for tag in repos_metas.keys():
			os.chdir(self.cwd)
			repo_metas = repos_metas[tag]
			base_dir = self.config.tag_directory(tag)
			ensure_path(base_dir)
			for (i, repo_meta) in enumerate(repo_metas):
				repo_dir = self.config.repo_directory(repo_meta)

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

	def load_metas_from_cache(self, repo_mappings:List[RepoMapping]) -> Dict[str, List[RepoMeta]]:
		repos_metas = dict()

		for repo_mapping in repo_mappings:
			repos_metas[repo_mapping.tag] = list(self.cache.load_metas(repo_mapping))
		return repos_metas

	def update_cache(self, repos_metas:Dict[str, List[RepoMeta]]) -> Dict[str, List[RepoMeta]]:

		for tag in repos_metas.keys():
			for (i, repo_meta) in enumerate(repos_metas[tag]):
				repo_meta = self.cache.load_meta(repo_meta)
				repos_metas[tag][i].last_commit_hash = self.cache.update_commits(repo_meta)
		
		return repos_metas

	def calculate_timestamp(self, commit_timestamp:int, period_interval:int) -> int:
		periods = int(commit_timestamp / period_interval) + 1
		return periods * period_interval

	# Generate stats for a single repository
	def generate_repository_stats(self, period_interval:int, repo_meta:RepoMeta=None, commits:List[gitparse.CommitData]=None) -> GitStatRepository:
		stats = dict()
		current = None
		if not commits:
			assert(repo_meta)
			commits = self.cache.get_commits(repo_meta)
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