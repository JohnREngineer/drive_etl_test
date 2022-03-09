from re import S
from tokenize import PseudoToken
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread
from oauth2client.client import GoogleCredentials
import pandas as pd
import time
import os
import json
import uuid
import random

def get_gspread_auth(gc=None):
  return gspread.authorize(gc or GoogleCredentials.get_application_default())

def get_drive_auth(gc=None):
  gauth = GoogleAuth()
  gauth.credentials = gc or GoogleCredentials.get_application_default()
  drive = GoogleDrive(gauth)
  return drive

def get_auths():
  gc = GoogleCredentials.get_application_default()
  gspread_auth = get_gspread_auth(gc)
  drive_auth = get_drive_auth(gc)
  return gspread_auth, drive_auth

def download_drive_file(key, drive_auth=None):
  drive = drive_auth or get_drive_auth()
  f = drive.CreateFile({'id':sanitize_key(key)}) # creates file object not a new file
  f.FetchMetadata(fetch_all=True)
  path = f.metadata['title']
  f.GetContentFile(path)
  return path

def get_df_from_columns(df, columns):
  names, calculations, psuedonames = list(map(list,list(zip(*columns))))
  psuedonames = [p or n for n,p in zip(names,psuedonames)]
  nf = df[calculations].copy()
  nf.columns = list(names)
  return nf, psuedonames

def sanitize_key(key):
  new_key = key
  if '/d/' in key:
    new_key = key.split('/')[-2]
  elif 'folders/' in key:
    new_key = key.split('folders/')[-1].split('?')[0]
  return new_key

def get_df_from_drive(location, defaults={'sheet':0, 'headers':0, 'start':1, 'end':None}, gspread_auth=None):
  gc = gspread_auth or get_gspread_auth()
  wb = gc.open_by_key(sanitize_key(location['key']))
  l = location
  l.update({k: v for k, v in defaults.items() if not location.get(k)})
  sheet = l['sheet']
  sh = wb.get_worksheet(int(sheet)) if str(sheet).isnumeric() else wb.worksheet(sheet)
  if not sh:
    raise ValueError('Worksheet cannot be found at '+str(location))
  df = pd.DataFrame(sh.get_all_values())
  df.columns = df.iloc[l['headers']]
  df = df.iloc[l['start']:l['end']]
  df = df.reset_index(drop=True)
  return df, sh

def if_then_else(inputs, values):
  iter_inputs = iter(inputs)
  question = next(iter_inputs)
  # This accounts for v == '' as a legitimate option
  answers = [next(iter_inputs) if (v is None) else v for v in values]
  return answers[not question]

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

def apply_function(df, config):
  function = config['function']
  input_column = config['input']
  args = config.get('args')
  kwargs = config.get('kwargs')
  functions = {
    'identity': lambda x: x,
    'constant': lambda x, y: y,
    'strip_upper': lambda x: x.strip().upper(),
    'concat': lambda x: ' '.join(filter(None,x)),
    'zfill': lambda x, y: x.zfill(y),
    'strip_left': lambda x, y: x.lstrip(y),
    'use_dictionary': lambda x, y, z: y.get(x,z),
    'if_then_else': if_then_else,
    'get_proxy_cat_no': get_proxy_cat_no,
  }
  f = functions[function]
  input = input_column
  if input_column is None:
    input = df.columns.values[0]
  elif str(input_column).isdigit():
    input = df.columns[input_column]
  apply_kwargs = {}
  if args is not None:
    apply_kwargs['args'] = args
  if kwargs != None:
    apply_kwargs.update(kwargs)
  if isinstance(input, list):
    apply_kwargs['axis'] = 1
  return df[input].apply(f, **apply_kwargs)

def export_to_template(path, sheet_name, df, nick_names, suffix):
  ef = pd.read_excel(path,sheet_name=sheet_name)
  ef.columns = df.columns
  ef = ef.append(df, ignore_index=True)
  ef.columns = nick_names
  with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    ef.to_excel(writer, sheet_name=sheet_name, index=False)
  new_path = 'New_'+sheet_name+'_'+suffix+'.xlsx'
  os.rename(path, new_path)
  return new_path

def export_dataframe(df, exports, columns, gspread_auth=None, drive_auth=None):
  exports = []
  suffix = str(int(time.time()))
  for export, cols in zip(exports,columns):
    nf, nick_names = get_df_from_columns(df, cols)
    unique_column = export.get('unique_column')
    nf['python_deduplicate_column'] = apply_function(nf, unique_column)
    ef = nf
    reference_dataset = export.get('reference_dataset')
    if reference_dataset:
      rf, list_sheet = get_df_from_drive(export['reference_dataset'], gspread_auth=gspread_auth)
      list_dedup = apply_function(rf, unique_column)
      ef = nf.loc[[(u not in list_dedup.values) for u in nf['python_deduplicate_column']]].copy()
    ef = ef.drop_duplicates(subset='python_deduplicate_column', keep='last')
    ef = ef.drop('python_deduplicate_column', axis=1)
    excel = export['excel']
    path = download_drive_file(sanitize_key(excel['key']), drive_auth)
    sheet_name = excel['sheet']
    if str(sheet_name).isdigit():
      xl = pd.ExcelFile(path)
      sheet_name = xl.sheet_names[int(sheet_name)]
    print('\tNew '+sheet_name+':\t'+str(len(ef)))
    export_path = None
    if len(ef) > 0:
      if reference_dataset:
        for _, row in ef.iterrows():
            list_sheet.append_rows(values=[list(row.values)])
      export_path = export_to_template(path, sheet_name, ef, nick_names, suffix)
      print('\t\tCreated '+export_path)
    exports.append([ef, export_path])
  return list(map(list,list(zip(*exports))))

def split_all(string, split_chars):
  out_string = string
  for s in split_chars:
    out_string = out_string.split(s)[0]
  return out_string

def get_df_from_inputs(inputs, defaults, calculations, gspread_auth=None, split_chars = ['\n','?','(']):
  dfs = []
  for input in inputs:
    print('\t'+'https://docs.google.com/spreadsheets/d/'+input['key']+'/edit')
    af = get_df_from_drive(input, defaults=defaults, gspread_auth=gspread_auth)[0]
    af.columns = [split_all(c, split_chars).strip().upper() for c in af.columns] 
    dfs.append(af)
  df = pd.concat(dfs)
  if len(df) == 0:
    return None
  for calc in calculations:
    df[calc['name']] = apply_function(df, calc)
    required_values = calc.get('required_values')
    if required_values is not None:
      non_compliant = df.loc[[c not in required_values for c in df[calc['name']]]]
      if len(non_compliant) > 0:
        raise ValueError('Noncompliant value found in the following row(s): '+
                         ', '.join([str(n+input['start']+1) for n in non_compliant.index.values]))
  return df

def load_json(path):
  with open(path, 'r') as f:
    return json.load(f)

def get_settings_from_key(key, drive_auth=None):
  key = sanitize_key(key)
  settings = load_json(download_drive_file(key, drive_auth=drive_auth))
  print('\tLoading https://drive.google.com/file/d/'+key+'/edit')
  return settings

def get_settings_from_folder(key, drive_auth=None):
  drive = drive_auth or get_drive_auth()
  key = sanitize_key(key)
  files = drive.ListFile({'q': "'{}' in parents and trashed=false".format(key)}).GetList()
  settings = []
  for f in files:
    if f.get('mimeType') == 'application/json':
      settings.append({'date':f.get('createdDate'), 'key':f.get('id'), 'title':f.get('title')})
  if not settings:
    return None
  first = sorted(settings, key=lambda x: x.get('date'), reverse=True)[0]
  print('\tFound '+first['title']+' in https://drive.google.com/drive/folders/'+key)
  settings = get_settings_from_key(first.get('key'), drive_auth=drive_auth)
  return settings

def get_settings(settings_location, drive_auth=None):
  print('Settings:')
  settings_getters = {
      'object': lambda s: s['object'],
      'path': lambda s: load_json(s['path']),
      'key': lambda s: get_settings_from_key(s.get('key'), drive_auth=drive_auth),
      'folder': lambda s: get_settings_from_folder(s.get('key'), drive_auth=drive_auth)
  }
  return settings_getters[settings_location['type']](settings_location)

def get_inputs_from_sheet(location, defaults={'sheet':0, 'headers':0, 'start':1, 'end':None}, gspread_auth=None):
  records = get_df_from_drive(location, gspread_auth=gspread_auth)[0].to_dict('records')
  new = []
  for index, row in enumerate(records):
    base = defaults.copy()
    transforms = {
      'key': sanitize_key,
      'sheet': lambda x: x,
      'headers': int,
      'start': int,
      'end': lambda x: None if ('none' in x.lower()) else x
    }
    update = {key: transforms.get(key)(value) for key, value in row.items() if value}
    base.update(update)
    new.append(base)
  return new

def get_inputs_from_folder(location, drive_auth=None):
  drive = drive_auth or get_drive_auth()
  files = drive.ListFile({'q': "'{}' in parents and trashed=false".format(sanitize_key(location['key']))}).GetList()
  inputs = [{'key':f.get('id')} for f in files if f.get('mimeType') == 'application/vnd.google-apps.spreadsheet']
  return inputs

def get_inputs(inputs, gspread_auth=None, drive_auth=None):
  inputs_getters = {
      'list': lambda i: i['list'],
      'sheet': lambda i: get_inputs_from_sheet(i['location'], defaults=i.get('defaults'), gspread_auth=gspread_auth),
      'folder': lambda i: get_inputs_from_folder(i['location'], drive_auth=drive_auth),
  }
  inputs_prints = {
      'list': lambda s:'Inputs passed directly.',
      'sheet': lambda s:'Inputs from sheet: https://docs.google.com/spreadsheets/d/'+s+'/edit',
      'folder': lambda s:'Inputs from folder: https://drive.google.com/drive/folders/'+s,
  }
  result = inputs_getters[inputs['type']](inputs)
  print(inputs_prints[inputs['type']](sanitize_key(inputs['location'].get('key',''))))
  return result

def get_nothing_response(n):
  return [None for _ in range(n)], []

def update_dataset(settings, gspread_auth=None, drive_auth=None):
  input_keys = get_inputs(settings['inputs'], gspread_auth, drive_auth)
  if input_keys:
    print('Inputs:')      
  else:
    print('No inputs found.')
    return get_nothing_response(len(settings['exports']))
  df = get_df_from_inputs(input_keys, settings['inputs']['defaults'], settings['calculations'], gspread_auth=gspread_auth)
  if df is None:
    print('\nAll input files are empty.')
    return get_nothing_response(len(settings['exports']))
  else:
    print('Results:')
  nfs, files = export_dataframe(df, settings['exports'], settings['columns'], gspread_auth=gspread_auth, drive_auth=drive_auth)
  return nfs, files

def update_datasets(settings_location):
  gspread_auth, drive_auth = get_auths()
  run_settings = get_settings(settings_location, drive_auth)
  if not run_settings:
    print('No settings found.')
    return None
  datasets = run_settings['datasets']
  ans = [update_dataset(d, gspread_auth=gspread_auth, drive_auth=drive_auth) for d in datasets]
  return ans