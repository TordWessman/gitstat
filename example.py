import logging
import pandas as pd
import matplotlib.pyplot as plt

import gitstat
from gitstat_pd import GitStatPd, GitStatData
from gitstat_models import *

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(gitstat.gitparse.GIT_PARSE_LOGGER_ID)
log.setLevel(logging.DEBUG) 

def display(stats:[GitStatData]):

	df = pd.DataFrame()
	for data in stats:
		if df.empty:
			df = data.df
		else:
			df = pd.merge(df, data.df, on="timestamp", how="inner")

	df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

	plt.figure(figsize=(10, 6))
	window_size = 3
	for data in stats:
		for column_name in data.columns:
			plt.plot(df['timestamp'], df[column_name].rolling(window=window_size).mean(), label=column_name, alpha=0.7)

	plt.title(f"Commits")
	plt.xlabel('Time')
	plt.ylabel('Commit count')
	plt.legend()
	plt.grid(True)

	plt.show()

def main():

	here_will_my_repositories_be_stored = "/var/tmp/git_parse_tmp/repos"
	here_will_results_be_cached = "/var/tmp/git_parse_tmp/cache"
	i_dont_want_to_include_any_commits_older_than_these = 1570277046 # Saturday, October 5, 2019 12:04:06 PM GMT
	time_resolution = 24 * 3600

	config = gitstat.GitStatConfig(repository_path=here_will_my_repositories_be_stored, cache_path=here_will_results_be_cached, max_history_time=i_dont_want_to_include_any_commits_older_than_these)
	gs = GitStatPd(config)
	
	repo_mappings = [RepoMapping(org="BonkLabs", tag="BONK")]
	
	for data in gs.synchronize(repo_mappings, time_resolution):
		print(data.df.describe())
	bonk = gs.load(GitStatData("BONK", time_resolution))
	display([bonk])

if __name__ == '__main__':
	main()