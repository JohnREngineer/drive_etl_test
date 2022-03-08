from re import S
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread
from oauth2client.client import GoogleCredentials
import pandas as pd
import time
import os
import json
from functools import reduce

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
  names, calculations = list(map(list,list(zip(*columns))))
  nf = df[calculations].copy()
  nf.columns = list(names)
  return nf

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

def export_to_template(df, excel, suffix, drive_auth=None):
  path = download_drive_file(sanitize_key(excel['key']), drive_auth)
  sheet_name = pd.ExcelFile(path).sheet_names[excel['sheet']] if isinstance(excel['sheet'], int) else excel['sheet']
  ef = pd.read_excel(path,sheet_name=sheet_name)
  ef = ef.append(df[ef.columns.values], ignore_index = True)
  with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    ef.to_excel(writer, sheet_name=sheet_name, index=False)
  new_path = 'New_'+sheet_name+'_'+suffix+'.xlsx'
  os.rename(path, new_path)
  return new_path, sheet_name

def if_then_else(inputs, values):
  iter_inputs = iter(inputs)
  question = next(iter_inputs)
  # This accounts for v == '' as a legitimate option
  answers = [next(iter_inputs) if (v is None) else v for v in values]
  return answers[not question]

def apply_function(df, function, input_value, args=None):
  functions = {
    'identity': lambda x: x,
    'constant': lambda x, y: y,
    'strip_upper': lambda x: x.strip().upper(),
    'concat': lambda x: ' '.join(filter(None,x)),
    'zfill': lambda x, y: x.zfill(y),
    'strip_left': lambda x, y: x.lstrip(y),
    'use_dictionary': lambda x, y, z: y.get(x,z),
    'if_then_else': if_then_else,
  }
  f = functions[function]
  input = input_value
  if input_value is None:
    input = df.columns.values[0]
  elif str(input_value).isdigit():
    input = df.columns[input_value]
  kwargs = {}
  if args is not None:
    kwargs['args'] = args
  if isinstance(input, list):
    kwargs['axis'] = 1
  return df[input].apply(f, **kwargs)

def export_unique(df, exports, gspread_auth=None, drive_auth=None):
  outputs = []
  suffix = str(int(time.time()))
  for export in exports:
    nf = get_df_from_columns(df, export['columns'])
    lf, list_sheet = get_df_from_drive(export['datatable'], gspread_auth=gspread_auth)
    unique = export['unique']
    list_dedup = apply_function(lf, unique['function'], unique['column'], unique.get('args'))
    nf['python_deduplicate_column'] = apply_function(nf, unique['function'], unique['column'], unique.get('args'))
    uf = nf.loc[[(u not in list_dedup.values) for u in nf['python_deduplicate_column']]].copy()
    uf = uf.drop_duplicates(subset='python_deduplicate_column', keep='last')
    uf = uf.drop('python_deduplicate_column', axis=1)
    path = None
    outputText = ''
    excel = export['excel']
    uniques = len(uf)
    if uniques > 0:
      for index, row in uf[list(lf.columns)].iterrows():
          list_sheet.append_rows(values=[list(row.values)])
      path, name = export_to_template(uf, excel, suffix, drive_auth)
      outputText = ', created '+path
    outputs.append([uf, path])
    print('\tNew '+name+':\t'+str(uniques)+outputText)
  return list(map(list,list(zip(*outputs))))

def get_df_from_inputs(inputs, defaults, calculations, gspread_auth=None):
  dfs = []
  for input in inputs:
    print('\t'+'https://docs.google.com/spreadsheets/d/'+input['key']+'/edit')
    dfs.append(get_df_from_drive(input, defaults=defaults, gspread_auth=gspread_auth)[0])
  df = pd.concat(dfs)
  if len(df) == 0:
    return None
  for calc in calculations:
    df[calc['name']] = apply_function(df, calc['function'], calc['input'], calc.get('args'))
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
  nfs, files = export_unique(df, settings['exports'], gspread_auth=gspread_auth, drive_auth=drive_auth)
  return nfs, files

def update_datasets(settings_location):
  gspread_auth, drive_auth = get_auths()
  run_settings = get_settings(settings_location, drive_auth)
  datasets = run_settings['datasets']
  if not datasets:
    print('No settings found.')
    return get_nothing_response(2)
  ans = [update_dataset(d, gspread_auth=gspread_auth, drive_auth=drive_auth) for d in datasets]
  return ans