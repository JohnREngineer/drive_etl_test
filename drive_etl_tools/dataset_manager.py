from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread
from oauth2client.client import GoogleCredentials
import pandas as pd
import time
import os
import json
import importlib.util

class DatasetManager:
  def __init__(self):
    self.__initialize_credentials()
    self.start_time_unix = ''
    self.functions = {}

  def __initialize_credentials(self):
    self.google_credentials = GoogleCredentials.get_application_default()
    self.gspread = gspread.authorize(self.google_credentials)
    gauth = GoogleAuth()
    gauth.credentials = self.google_credentials
    self.drive = GoogleDrive(gauth)

  def __download_drive_file(self, key):
    f = self.drive.CreateFile({'id':self.__sanitize_key(key)}) # creates file object not a new file
    f.FetchMetadata(fetch_all=True)
    path = f.metadata['title']
    f.GetContentFile(path)
    return path

  def __get_df_from_columns(self, df, columns):
    names, calculations, psuedonames = list(map(list,list(zip(*columns))))
    psuedonames = [p or n for n,p in zip(names,psuedonames)]
    nf = df[calculations].copy()
    nf.columns = list(names)
    return nf, psuedonames

  def __sanitize_key(self, key):
    new_key = key
    if '/d/' in key:
      new_key = key.split('/')[-2]
    elif 'folders/' in key:
      new_key = key.split('folders/')[-1].split('?')[0]
    return new_key

  def __get_df_from_drive(self, location, defaults={'sheet':0, 'headers':0, 'start':1, 'end':None}):
    wb = gspread.open_by_key(self.__sanitize_key(location['key']))
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

  def __apply_function(self, df, config):
    function = config['function']
    input_column = config['input']
    args = config.get('args')
    kwargs = config.get('kwargs')
    f = self.functions[function]
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

  def __export_to_template(self, path, sheet_name, df, nick_names):
    ef = pd.read_excel(path,sheet_name=sheet_name)
    ef.columns = df.columns
    ef = ef.append(df, ignore_index=True)
    ef.columns = nick_names
    with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
      ef.to_excel(writer, sheet_name=sheet_name, index=False)
    new_path = 'New_'+sheet_name+'_'+self.start_time_unix+'.xlsx'
    os.rename(path, new_path)
    return new_path

  def __export_dataframe(self, df, exports, columns):
    outputs = []
    for export, cols in zip(exports,columns):
      nf, nick_names = self.__get_df_from_columns(df, cols)
      unique_column = export.get('unique_column')
      nf['python_deduplicate_column'] = self.__apply_function(nf, unique_column)
      ef = nf
      reference_dataset = export.get('reference_dataset')
      if reference_dataset:
        rf, list_sheet = self.__get_df_from_drive(export['reference_dataset'])
        list_dedup = self.__apply_function(rf, unique_column)
        ef = nf.loc[[(u not in list_dedup.values) for u in nf['python_deduplicate_column']]].copy()
      ef = ef.drop_duplicates(subset='python_deduplicate_column', keep='last')
      ef = ef.drop('python_deduplicate_column', axis=1)
      excel = export['excel']
      path = self.__download_drive_file(self.__sanitize_key(excel['key']))
      sheet_name = excel['sheet']
      if str(sheet_name).isdigit():
        xl = pd.ExcelFile(path)
        sheet_name = xl.sheet_names[int(sheet_name)]
      print('\tNew '+sheet_name+':\t'+str(len(ef)))
      out_path = None
      if len(ef) > 0:
        if reference_dataset:
          for _, row in ef.iterrows():
              list_sheet.append_rows(values=[list(row.values)])
        out_path = self.__export_to_template(path, sheet_name, ef, nick_names)
        print('\t\tCreated '+out_path)
      outputs.append([ef, out_path])
    return list(map(list,list(zip(*outputs))))

  def __split_all(self, string, split_chars):
    out_string = string
    for s in split_chars:
      out_string = out_string.split(s)[0]
    return out_string

  def __get_df_from_inputs(self, inputs, defaults, calculations, split_chars = ['\n','?','(']):
    dfs = []
    for input in inputs:
      print('\t'+'https://docs.google.com/spreadsheets/d/'+input['key']+'/edit')
      af = self.__get_df_from_drive(input, defaults=defaults)[0]
      af.columns = [self.__split_all(c, split_chars).strip().upper() for c in af.columns] 
      dfs.append(af)
    df = pd.concat(dfs)
    if len(df) == 0:
      return None
    for calc in calculations:
      df[calc['name']] = self.__apply_function(df, calc)
      required_values = calc.get('required_values')
      if required_values is not None:
        non_compliant = df.loc[[c not in required_values for c in df[calc['name']]]]
        if len(non_compliant) > 0:
          raise ValueError('Noncompliant value found in the following row(s): '+
                          ', '.join([str(n+input['start']+1) for n in non_compliant.index.values]))
    return df

  def __load_json(self, path):
    with open(path, 'r') as f:
      return json.load(f)

  def __get_settings_from_key(self, key):
    key = self.__sanitize_key(key)
    print('\tLoading https://drive.google.com/file/d/'+key+'/edit')
    path = self.__download_drive_file(key)
    settings = self.__load_json(path)
    return settings

  def __get_settings_from_folder(self, key):
    key = self.__sanitize_key(key)
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(key)}).GetList()
    settings = []
    for f in files:
      if f.get('mimeType') == 'application/json':
        settings.append({'date': f.get('modifiedDate'), 'key': f.get('id'), 'title': f.get('title')})
    if not settings:
      return None
    first = sorted(settings, key=lambda x: x.get('date'), reverse=True)[0]
    print('\tFound '+first['title']+' in https://drive.google.com/drive/folders/'+key)
    settings = self.__get_settings_from_key(first.get('key'))
    return settings

  def __get_settings(self, settings_location):
    print('Settings:')
    settings_getters = {
        'object': lambda s: s['object'],
        'path': lambda s: self.__load_json(s['path']),
        'key': lambda s: self.__get_settings_from_key(s.get('key')),
        'folder': lambda s: self.__get_settings_from_folder(s.get('key'))
    }
    return settings_getters[settings_location['type']](settings_location)

  def __get_functions_from_key(self, key):
    key = self.__sanitize_key(key)
    path = self.__download_drive_file(key)
    spec = importlib.util.spec_from_file_location("functions", path)
    functions = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(functions)
    self.functions = functions.get_functions()
    print('\tLoading https://drive.google.com/file/d/'+key+'/edit')
    return functions

  def __get_functions_from_folder(self, key):
    key = self.__sanitize_key(key)
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(key)}).GetList()
    functions = []
    for f in files:
      print(f.get('mimeType'))
      if f.get('mimeType') != 'nothing':
        functions.append({'date': f.get('modifiedDate'), 'key': f.get('id'), 'title': f.get('title')})
    if not functions:
      return None
    first = sorted(functions, key=lambda x: x.get('date'), reverse=True)[0]
    print('\tFound '+first['title']+' in https://drive.google.com/drive/folders/'+key)
    functions = self.__get_functions_from_key(first.get('key'))
    return functions

  def __update_functions(self, functions_location):
    print('functions:')
    functions_getters = {
        'object': lambda s: s['object'],
        'path': lambda s: self.__load_json(s['path']),
        'key': lambda s: self.__get_functions_from_key(s.get('key')),
        'folder': lambda s: self.__get_functions_from_folder(s.get('key'))
    }
    self.functions = functions_getters[functions_location['type']](functions_location)

  def __get_inputs_from_sheet(self, location, defaults={'sheet':0, 'headers':0, 'start':1, 'end':None}):
    records = self.__get_df_from_drive(location)[0].to_dict('records')
    new = []
    for index, row in enumerate(records):
      base = defaults.copy()
      transforms = {
        'key': self.__sanitize_key,
        'sheet': lambda x: x,
        'headers': int,
        'start': int,
        'end': lambda x: None if ('none' in x.lower()) else x
      }
      update = {key: transforms.get(key)(value) for key, value in row.items() if value}
      base.update(update)
      new.append(base)
    return new

  def __get_inputs_from_folder(self, location):
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(self.__sanitize_key(location['key']))}).GetList()
    inputs = [{'key': f.get('id')} for f in files if f.get('mimeType') == 'application/vnd.google-apps.spreadsheet']
    return inputs

  def __get_inputs(self, inputs):
    inputs_getters = {
        'list': lambda i: i['list'],
        'sheet': lambda i: self.__get_inputs_from_sheet(i['location'], defaults=i.get('defaults')),
        'folder': lambda i: self.__get_inputs_from_folder(i['location']),
    }
    inputs_prints = {
        'list': lambda s:'Inputs passed directly.',
        'sheet': lambda s:'Inputs from sheet: https://docs.google.com/spreadsheets/d/'+s+'/edit',
        'folder': lambda s:'Inputs from folder: https://drive.google.com/drive/folders/'+s,
    }
    result = inputs_getters[inputs['type']](inputs)
    print(inputs_prints[inputs['type']](self.__sanitize_key(inputs['location'].get('key',''))))
    return result

  def __get_nothing_response(self, n):
    return [None for _ in range(n)], []

  def __update_start_time(self):
    self.startime = str(int(time.time()))

  def __update_dataset(self, settings):
    input_settings = settings['inputs']
    input_locations = self.__get_inputs(input_settings)
    export_settings = settings['exports']
    self.__update_functions(settings['functions'])
    if input_locations:
      print('Inputs:')      
    else:
      print('No inputs found.')
      return self.__get_nothing_response(len(export_settings))
    df = self.__get_df_from_inputs(input_locations, input_settings['defaults'], settings['calculations'])
    if df:
      print('Results:')
    else:
      print('\nAll input files are empty.')
      return self.__get_nothing_response(len(export_settings))
    output = self.__export_dataframe(df, export_settings, settings['columns'])
    return output

  def run_update(self, settings_location):
    self.__update_start_time()
    run_settings = self.__get_settings(settings_location)
    if not run_settings:
      print('No settings found.')
      return None
    datasets = run_settings['datasets']
    output = [self.__update_dataset(d) for d in datasets]
    return output