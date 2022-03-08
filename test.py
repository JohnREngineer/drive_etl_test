import random
import uuid

def get_proxy_cat_no(inputs, index_combinations, length=16, prefix='PROXY', dashed=True):
  seed = str(uuid.uuid4())
  for c in index_combinations:
    if all(inputs[c]):
      seed = str(inputs[c].values)
      break
  random.seed(seed)
  random_number = int(10**length*random.random())
  random_padded = str(random_number).zfill(length)
  random_dashed = random_padded[:4]+'-'+random_padded[4:8]+'-'+random_padded[8:12]+'-'+random_padded[12:]
  cat_no = prefix+('-'+random_dashed if dashed else random_padded)
  return cat_no

import pandas as pd

def main():
  examples = 12
  
  print(all(['asdf','']))
  df = pd.DataFrame({
    'upc':          ['19075-95055-2'                if i < 2 else '' for i in range(0,examples)],
    'label':        ['Tool Dissectional'  if i < 6 else '' for i in range(0,examples)],
    'artist':       ['Tool'               if i < 10 else '' for i in range(0,examples)],
    'album_title':  ['Fear Inoculum'      if i < 4 else '' for i in range(0,examples)],
    'release_date': ['08/30/2019'         if i < 8 else '' for i in range(0,examples)],
  })
  
  input = ['upc','label','artist','album_title','release_date']
  combinations = [[0],[3,2,1],[4,2,1],[4,1],[4,2]]
  args = [combinations]
  kwargs = {'dashed':False}
  x = {
    'args': args,
    'axis': 1,
  }
  x.update(kwargs)
  print(x)  
  df['rnd'] = df[input].apply(get_proxy_cat_no, **x)
  
  return df


af = main()
bf = main()

print(af)
print(bf)