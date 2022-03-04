from .tools.update import update_datasets
import sys
from google.colab import auth, files
from pathlib import Path

def run_update(config, download=True):
  if not Path("adc.json").is_file(): # adc.json is created upon authentication
    auth.authenticate_user()
  dfs, paths = update_datasets(config)
  for p in paths:
    if p and download:
      files.download(p)
  return dfs
  
if __name__ == '__main__':
  print('Hey.')
  run_update(sys.argv)