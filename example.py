import logging
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict

from gitstat import *
from gitstat_pd import GitStatPd, GitStatData
import gitstat

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(gitstat.LOGGER_TAG)
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
	window_size = 30
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

	here_will_my_repositories_be_stored = "~/crypto_temp/crypto_repos"
	here_will_results_be_cached = "~/crypto_temp/cache"
	i_dont_want_to_include_any_commits_older_than_these = int(datetime(2019, 9, 20, 10, tzinfo=pytz.utc).timestamp())
	time_resolution = 24 * 3600

	config = GitStatConfig(here_will_my_repositories_be_stored, i_dont_want_to_include_any_commits_older_than_these, cache_path=here_will_results_be_cached)
	gs = GitStatPd(config)
	
	repo_mappings = [RepoMapping(org="bit-country", tag="NUUM"), RepoMapping(org="celestiaorg", tag="TIA", max_count=50), RepoMapping(org="aptos-labs", tag="APT"), RepoMapping(org="iotexproject", tag="IOTEX", ignore_repos=["homebrew-core", "arduino-sdk"], max_count=50), RepoMapping(org="InvArch", tag="VARCH")]
	
	for data in gs.synchronize(repo_mappings, time_resolution):
		print(data.df.describe())
	tia = gs.load(GitStatData("TIA", time_resolution))
	varch = gs.load(GitStatData("VARCH", time_resolution))
	display([varch, tia])
	

if __name__ == '__main__':
	main()