import logging
import os

# Ensure that the specified is a valid directory
def ensure_path(path):
	if not os.path.isdir(path):
		current_path = ""
		for path_component in path.split('/'):
			current_path = os.path.join(current_path, path_component)

			# handle absolute paths
			if current_path == '':
				if path[0] == "/":
					current_path = "/"
					continue
				else:
					raise Exception(f"Path component of '{path}' was empty.")
			if os.path.isfile(current_path):
				raise Exception(f"{current_path} was a file. Unable to create directory")
			elif not os.path.isdir(current_path):
				logging.info(f"Creating '{current_path}'")
				os.mkdir(current_path)
