import os
import copy
import subprocess
import datetime
import re
import concurrent.futures
import multiprocessing
from dateutil import parser as date_parser
from typing import List, Dict
import logging

GIT_PARSE_LOGGER_ID = "gitparse_logger"

log = logging.getLogger(GIT_PARSE_LOGGER_ID)

class UnexpectedLineError(Exception):
	def __init__(self, line):
		super(UnexpectedLineError, self).__init__('ERROR: Unexpected Line: ' + line)

class Author(object):

	def __init__(self, name:str="", email:str=""):
		self.name = name
		self.email = email

	def to_dict(self):
		return {
			'name' : self.name,
			'email' : self.email,
		}
	
	def __str__(self):
		return f"{self.name} : {self.email}"

	def __eq__(self, other):
		return self.name == other.name and self.email == other.email

class CommitData(object):

	def __init__(self, commit_hash:str=None, author:Author=Author(), message:str=None,
				 date:int=None, is_merge:bool=False, change_id:str=None, files_changed:int=0, insertions:int=0, deletions:int=0, db_row=None):
		if db_row:
			_, _, _, self.date, self.insertions, self.deletions, self.commit_hash = db_row
			self.author = Author()	
		else:
			self.commit_hash = commit_hash
			self.author = author
			self.message = message
			self.date = date
			self.is_merge = is_merge
			self.ignore = False
			self.change_id = change_id
			self.files_changed = files_changed
			self.insertions = insertions
			self.deletions = deletions

	def __str__(self):
		return f"{self.commit_hash}, {self.author}, {self.message}, {self.date}, {self.is_merge}, {self.change_id}, {self.files_changed}, {self.insertions}, {self.deletions}"

	def __eq__(self, other):
		if isinstance(other, CommitData):
			return (self.commit_hash == other.commit_hash 
				and self.author == other.author 
				and self.message == other.message 
				and self.date == other.date
				and self.is_merge == other.is_merge 
				and self.change_id == other.change_id
				and self.files_changed == other.files_changed
				and self.insertions == other.insertions
				and self.deletions == other.deletions)


def parse_datetime(date_string:str):

	date = date_parser.parse(date_string)
	return int(datetime.datetime.timestamp(date))

def get_output(command:str, base_path:str):
	res_string = ""
	with subprocess.Popen(command, stdout=Nonesubprocess.PIPE, stderr=None, shell=True, cwd=base_path) as process:
		return process.communicate()[0].decode("utf-8").rstrip()

class GitLogParser():

	def __init__(self, repository_directory:str=".", last_hash:str=None, start_date:int=None):
		self.commits = []
		self.repository_directory = repository_directory
		self.stop_at_hash = last_hash
		self.start_date = start_date

	def mine_stats(self, commit_hash:str):
		parent = subprocess.getoutput('git log --pretty=%P -1 ' + commit_hash)
		return subprocess.getoutput('git diff ' + parent + ' ' + commit_hash + ' --shortstat')

	def parse_commit_hash(self, next_line:int, commit:CommitData):
		# commit xxxx
		if commit.commit_hash is not None:
			# new commit, reset object
			self.commits.append(copy.deepcopy(commit))
			commit = CommitData()
		commit.commit_hash = re.match('commit (.*)', next_line, re.IGNORECASE).group(1)

		return commit

	def parse_author(self, next_line:str):
		m = re.compile('Author: (.*) <(.*)>').match(next_line)
		return Author(m.group(1), m.group(2))

	def parse_date(self, next_line:str, commit:str):
		# Date: xxx
		m = re.compile(r'Date:\s+(.*)$').match(next_line)
		commit.date = parse_datetime(m.group(1))

	def parse_commit_msg(self, next_line:str, commit:CommitData):
		# (4 empty spaces)
		if commit.message is None:
			commit.message = next_line.strip()
		else:
			commit.message = commit.message + os.linesep + next_line.strip()

		if 'merge' in commit.message or 'Merge' in commit.message:
			commit.is_merge = True

	def parse_change_id(self, next_line:str, commit:CommitData):
		commit.change_id = re.compile(r'    Change-Id:\s*(.*)').match(next_line).group(1)

	def parse_lines(self, raw_lines:List[str], commit:CommitData=None):
		if commit is None:
			commit = CommitData()
		log.info("Parsing lines: %s", self.repository_directory)
		for next_line in raw_lines.splitlines():
			
			if len(next_line.strip()) == 0:
				# ignore empty lines
				pass

			elif bool(re.match('commit', next_line, re.IGNORECASE)):
				commit_orig = self.parse_commit_hash(next_line, commit)
				if self.stop_at_hash is not None and commit_orig.commit_hash == self.stop_at_hash:
					log.info("%s will stop at hash: %s", self.repository_directory,  self.stop_at_hash)
					if len(self.commits) > 0:
						return self.commits[-1]
					else:
						return None
				commit = copy.deepcopy(commit_orig)

			elif bool(re.match('merge:', next_line, re.IGNORECASE)):
				pass

			elif bool(re.match('author:', next_line, re.IGNORECASE)):
				commit.author = self.parse_author(next_line)

			elif bool(re.match('date:', next_line, re.IGNORECASE)):
				self.parse_date(next_line, commit)

			elif bool(re.match('    ', next_line, re.IGNORECASE)):
				self.parse_commit_msg(next_line, commit)

			elif bool(re.match('    change-id: ', next_line, re.IGNORECASE)):
				self.parse_change_id(next_line, commit)
			else:
				log.exception("UnexpectedLineError(%s)", next_line)

			if self.start_date is not None and commit.date is not None and commit.date < self.start_date:
				commit.ignore = True
				
		if len(self.commits) != 0 and not commit.ignore:
			if not len(commit_orig.commit_hash ) == 40 or not bool(re.match(r'[a-z0-9]+', commit_orig.commit_hash)):
				log.warning("Commit hash '%s' is not a valid hash. Ignoring.", commit.commit_hash)
				commit.ignore = True
			else:
				self.commits.append(commit)

		return commit

	def update_stats(self):

		cwd = os.getcwd()
		os.chdir(self.repository_directory)

		j = 0
		try:
			with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
				results = list()
				
				log.info("%s Mining stats", self.repository_directory)
				for i in range(len(self.commits)-2, -1, -1):
					results.append(executor.submit(self.mine_stats, self.commits[i].commit_hash))
			
				#this is needed since the commits are in a different order then the results
				current_commit = len(self.commits) - 2
			
				log.info("%s Getting diff-data", self.repository_directory)
				for r in results:
#					j = j + 1
#					if j % 100 == 0:
#						print(f"{j}")
					if self.commits[current_commit].is_merge:
						resultList = r.result()
						if len(resultList) < 3:
							log.warning(f"Unable to parse diff data from merge: {self.commits[current_commit].commit_hash} in {self.repository_directory}")
							continue
						if not resultList[0] == ' ':
							self.commits[current_commit].files_changed = int(resultList[0])
						if not resultList[1] == ' ':
							self.commits[current_commit].insertions = int(resultList[1])
						if not resultList[2] == ' ':
							self.commits[current_commit].deletions = int(resultList[2])

					else:
						stat_dict = dict()
						# since the result method stop the code until the thread finishes, we don't have to wait for the results to come in anywhere else
						stats = r.result().split()
						# since all 3 stats can be 0 in which case they are not displayed, this loop is needed to create a dict based on the existing ones
						for j in range(1, len(stats)):
							if stats[j-1].isdigit():
								stat_dict[stats[j]] = int(stats[j-1])
						#if a part of a statistic is missing the keys vary, but they always start the same way
						for key in stat_dict:
							if key.startswith('file'):
								self.commits[current_commit].files_changed = int(stat_dict[key])

							if key.startswith('insertion'):
								self.commits[current_commit].insertions = int(stat_dict[key])

							if key.startswith('deletion'):
								self.commits[current_commit].deletions = int(stat_dict[key])

							self.commits[current_commit].total = self.commits[current_commit].deletions + self.commits[current_commit].insertions 
					current_commit = current_commit - 1
		except:
			os.chdir(cwd)
			raise

			os.chdir(cwd)

def get_commits(repository_directory, last_hash:int=None, start_date:int=None):
	try:
		git_result = subprocess.check_output(['git', 'log'], cwd=repository_directory)
	except subprocess.CalledProcessError as e:
		log.error(f"{repository_directory} Git process error: {e}")
		return []
	decoded = git_result.decode("utf8", 'ignore')
	parser = GitLogParser(repository_directory, last_hash, start_date)
	parser.parse_lines(decoded)

	if not len(parser.commits) == 0:
		parser.update_stats()
	return parser.commits