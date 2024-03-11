import pandas as pd
import os
from typing import List, Dict
import logging

import gitstat
from common import ensure_path

log = logging.getLogger(gitstat.LOGGER_TAG)

class GitStatData:

	def __init__(self, tag:str, interval:int, df:pd.DataFrame = pd.DataFrame(), columns:[str] = []):
		self.tag = tag
		self.interval = interval
		self._df = df
		self.columns = columns

	@staticmethod
	def get_column_name(original_name:str, tag:str, interval:int):
		return f"{original_name}_{tag}_{interval}"

	@property
	def df(self):
		return self._df

	@df.setter
	def df(self, value:pd.DataFrame):
		self._df = value
		if not self._columns:
			self._columns = [next(col for col in self._df.columns if col != "timestamp")]

	@property
	def columns(self):
		return self._columns

	@columns.setter
	def columns(self, value:[str]):
		self._columns = [GitStatData.get_column_name(col, self.tag, self.interval) for col in value]

	@property
	def file_name(self) -> str:
		return f"{self.tag}_{self.interval}"

	@property
	def available_column_names(self) -> List[str]:
		all_columns = [col for col in self.df.columns if col != "timestamp"]


class GitStatPd:

	def __init__(self, config: gitstat.GitStatConfig):
		self.config = config
		self.gitstat = gitstat.GitStats(config)

	def _save_frame(self, data:GitStatData):
		pd.DataFrame.to_pickle(data.df, self._file_name_for(data))

	def _file_name_for(self, data:GitStatData):
		ensure_path(self.config.cache_path)
		return os.path.join(self.config.cache_path, f"{data.file_name}.pkl")

	def load(self, data:GitStatData) -> GitStatData:
		data.df = pd.read_pickle(self._file_name_for(data))
		return data

	def synchronize(self, source:List[gitstat.RepoMapping], interval:int) -> [GitStatData]:
		repositories_metas = self.gitstat.fetch_repositories_meta(source)
		repositories_metas = self.gitstat.download_source_code(repositories_metas)
		for tag, repo_metas in repositories_metas.items():
			stats_objects =  self.gitstat.generate_stats(interval, repo_metas)
			stats_dicts = [entry.as_dict for entry in stats_objects]
			stats = pd.DataFrame(stats_dicts)
			#stats.to_pickle(self._file_name_for(tag, interval))
			stats = stats.rename(columns=lambda x: GitStatData.get_column_name(x, tag, interval) if x != "timestamp" else x).sort_values(by="timestamp")
			data = GitStatData(tag, interval, stats)
			self._save_frame(data)
			
			yield GitStatData(tag, interval, stats)
