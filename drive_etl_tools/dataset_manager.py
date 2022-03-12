import pathlib
from re import I
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread
from oauth2client.client import GoogleCredentials
import pandas as pd
import time
import uuid
import os
import json
import importlib
import sys

class DatasetManager:
  def __init__(self):
    self.start_time_unix = ''
    self.etl_functions = {}
    self.__update_credentials()

  def __update_credentials(self):
    self.google_credentials = GoogleCredentials.get_application_default()
    self.gss_client = gspread.authorize(self.google_credentials)
    gauth = GoogleAuth()
    gauth.credentials = self.google_credentials
    self.drive = GoogleDrive(gauth)

  def __download_drive_file(self, key):
    f = self.drive.CreateFile({'id':self.__sanitize_key(key)}) # creates file object not a new file
    f.FetchMetadata(fetch_all=True)
    path = f.metadata['title']
    f.GetContentFile(path)
    return path
  
  def __get_output_from_columns(self, df, columns):
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

  def __get_df_from_drive(self, key=None, sheet=0, headers=0, start=1, end=None):
    df, sh = None, None
    if key:
      wb = self.gss_client.open_by_key(self.__sanitize_key(key))
      sh = wb.get_worksheet(int(sheet)) if str(sheet).isnumeric() else wb.worksheet(sheet)
      if not sh:
        raise ValueError('Worksheet cannot be found at %'%(key))
      df = pd.DataFrame(sh.get_all_values())
      df.columns = df.iloc[int(headers)]
      df = df.iloc[int(start):(int(end) if end else None)]
      df = df.reset_index(drop=True)
    return df, sh

  def __apply_function(self, df, name=None, inputs=None, function=None, args=None, kwargs=None, inplace=False):
    print(df.columns.values)
    apply_function = self.etl_functions[function]
    #    Can't be '' here because '' could be an input column
    input_columns = df.columns.values[0] if (inputs is None) else inputs
    if str(inputs).isdigit():
      input_columns = df.columns.values[inputs]
    # Define args and kwargs
    apply_kwargs = {}
    if args is not None:
      apply_kwargs['args'] = args
    if kwargs is not None:
      apply_kwargs.update(kwargs)
    if isinstance(input_columns, list):
      apply_kwargs['axis'] = 1
    # Apply function to dataframe with input, args, and kwargs
    new_column = df[input_columns].apply(apply_function, **apply_kwargs)
    if inplace:
      df_new = df.copy()
      df_new[name] = new_column
      return df_new
    else:
      return new_column

  def __split_all(self, string, split_chars):
    out_string = string
    for s in split_chars:
      out_string = out_string.split(s)[0]
    return out_string

  def __load_json(self, path):
    with open(path, 'r') as f:
      return json.load(f)

  def __get_settings_from_key(self, key):
    key = self.__sanitize_key(key)
    path = self.__download_drive_file(key)
    settings = self.__load_json(path)
    print('\tLoaded settings from https://drive.google.com/file/d/'+key+'/edit')
    return settings

  def __get_settings_from_folder(self, key):
    # Get all files in folder
    key = self.__sanitize_key(key)
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(key)}).GetList()
    # Find latest settings file
    settings = []
    for f in files:
      if f.get('mimeType') == 'application/json':
        settings.append({'date': f.get('modifiedDate'), 'key': f.get('id'), 'title': f.get('title')})
    first = sorted(settings, key=lambda x: x.get('date'), reverse=True)[0]
    # Report findings
    if not settings:
      raise ValueError('No settings found in folder https://drive.google.com/drive/folders/%s'%(key))
    else:
      print('\tFound '+first['title']+' in https://drive.google.com/drive/folders/'+key)
    # Get settings
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

  def __import_module_from_path(self, path):
    module_path = pathlib.Path(path).resolve()
    module_name = module_path.stem  # 'path/x.py' -> 'x'
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

  def __update_functions_from_key(self, key):
    key = self.__sanitize_key(key)
    print('\tLoading https://drive.google.com/file/d/'+key+'/edit')
    path = self.__download_drive_file(key)
    functions = self.__import_module_from_path(path)
    self.etl_functions = functions.get_etl_functions()

  def __update_functions_from_folder(self, key):
    key = self.__sanitize_key(key)
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(key)}).GetList()
    functions = []
    for f in files:
      if f.get('mimeType') == 'text/x-python':
        functions.append({'date': f.get('modifiedDate'), 'key': f.get('id'), 'title': f.get('title')})
    if not functions:
      return None
    first = sorted(functions, key=lambda x: x.get('date'), reverse=True)[0]
    print('\tFound '+first['title']+' in https://drive.google.com/drive/folders/'+key)
    functions = self.__update_functions_from_key(first.get('key'))
    return functions

  def __update_functions(self, functions_location):
    print('Functions:')
    functions_getters = {
        'object': lambda s: s['object'],
        'path': lambda s: self.__load_json(s['path']),
        'key': lambda s: self.__update_functions_from_key(s.get('key')),
        'folder': lambda s: self.__update_functions_from_folder(s.get('key'))
    }
    functions_getters[functions_location['type']](functions_location)

  def __get_inputs_from_sheet(self, location, sheet=0, headers=0, start=1, end=None):
    input_locations = self.__get_df_from_drive(**location)[0].to_dict('records')
    processed_input_locations = []
    for _, row in enumerate(input_locations):
      base = {
        'sheet': sheet,
        'headers': headers,
        'start': start,
        'end': end
      }
      processing_functions = {
        'key': self.__sanitize_key,
        'sheet': str,
        'headers': int,
        'start': int,
        'end': lambda x: None if ('none' in x.lower()) else x
      }
      processed_input = {key: processing_functions.get(key)(value) for key, value in row.items() if value}
      base.update(processed_input)
      processed_input_locations.append(base)
    return processed_input_locations

  def __get_inputs_from_folder(self, location):
    files = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(self.__sanitize_key(location['key']))}).GetList()
    inputs = [{'key': f.get('id')} for f in files if f.get('mimeType') == 'application/vnd.google-apps.spreadsheet']
    return inputs

  def __get_input_locations(self, inputs):
    inputs_getters = {
        'list': lambda i: i['list'],
        'sheet': lambda i: self.__get_inputs_from_sheet(i['location'], **i['defaults']),
        'folder': lambda i: self.__get_inputs_from_folder(i['location']),
    }
    inputs_prints = {
        'list': lambda i:'Inputs passed directly.',
        'sheet': lambda i:'Inputs from sheet: https://docs.google.com/spreadsheets/d/%s/edit' % (self.__sanitize_key(i['location']['key'])),
        'folder': lambda i:'Inputs from folder: https://drive.google.com/drive/folders/%s' % (self.__sanitize_key(i['location']['key'])),
    }
    result = inputs_getters[inputs['type']](inputs)
    print(inputs_prints[inputs['type']](inputs))
    return result

  def __get_empty_output(self, n):
    return [None for _ in range(n)], []

  def __update_start_time(self):
    self.start_time_unix = str(int(time.time()))

  def __apply_filters(self, input_df, filters):
    df = input_df.copy()
    if filters:
      filtered_df = df.copy()
      for my_filter in filters:
        filtered_df = filtered_df.loc[self.__apply_function(filtered_df, **my_filter)]
      df = filtered_df
    return df

  def __deduplicate_dataset(self, input_df, dedup_column, parent_dataset_location=None):
    df = input_df.copy()
    if dedup_column:
      my_dedup_column = str(uuid.uuid4()) # ensure that this column name is not overwriting an input column
      df[my_dedup_column] = self.__apply_function(df, **dedup_column)
      # Check against parent dataset for duplicates
      if parent_dataset_location:
        parent_df, parent_sheet = self.__get_df_from_drive(**parent_dataset_location)
        dedup_list = self.__apply_function(parent_df, **dedup_column).values
        df = df.loc[[(u not in dedup_list) for u in df[my_dedup_column]]].copy()
      # Drop internal duplicates
      df.drop_duplicates(subset=my_dedup_column, keep='last', inplace=True)
      df.drop(my_dedup_column, axis=1, inplace=True)
    return df, parent_sheet

  def __export_to_excel(self, df, path, excel_location, nick_names=None):
    template_path = self.__download_drive_file(self.__sanitize_key(excel_location['key']))
    os.rename(template_path, path)
    sheet_name = excel_location.get('sheet',0)
    ef = pd.read_excel(path, sheet_name)
    ef.columns = df.columns
    ef = ef.append(df, ignore_index=True)
    if nick_names:
      ef.columns = nick_names
    with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
      ef.to_excel(writer, sheet_name, index=False)
    print('\t\tCreated %s'%(path))

  def __upload_file_to_folder(self, path, folder=None):
    if folder.get('key'):
      sanitized_key = self.__sanitize_key(folder['key'])
      f = self.drive.CreateFile({'parents': [{'kind': 'drive#fileLink', 'id': sanitized_key}]})
      f.SetContentFile(path)
      f.Upload()
      print('\t\tUploaded %s to https://drive.google.com/drive/folders/%s',(path, sanitized_key))

  def __append_to_parent_sheet(self, df, parent_sheet=None):
    if parent_sheet:
      for _, row in df.iterrows():
        parent_sheet.append_rows(values=[list(row.values)])
      print('Appended new data to parent dataset.')

  def __get_outputs_from_dataset(self, input_df, output_settings):
    if (input_df is None) or (len(input_df) == 0) :
      return self.__get_empty_output(len(output_settings))
    outputs = []
    for o in zip(output_settings):
      df, nick_names = self.__get_output_from_columns(input_df, o['columns'])
      df, parent_sheet = self.__deduplicate_dataset(df, o.get('dedup_column'), o.get('parent_dataset'))
      print('\tNew %s:\t%s' % (o['name'], len(df)))
      df = self.__apply_filters(df, o.get('filters'))
      path = None
      if len(df) > 0:
        path = 'New_%s_%s.xlsx'%(o['name'], self.start_time_unix)
        self.__append_to_parent_sheet(df, parent_sheet)
        self.__export_to_excel(df, path, nick_names)
        self.__upload_file_to_folder(path, o.get('folder'))
      outputs.append([df, path])
    transposed_outputs = list(map(list,list(zip(*outputs)))) 
    return transposed_outputs

  def __get_dataset_from_input_locations(self, input_locations, defaults=None, split_chars = ['\n','?','(']):
    dfs = []
    print('Inputs:')
    for location in input_locations:
      print('\t'+'https://docs.google.com/spreadsheets/d/'+location['key']+'/edit')
      full_location = defaults.copy()
      full_location.update(location)
      af = self.__get_df_from_drive(**full_location)[0]
      af.columns = [self.__split_all(c, split_chars).strip().upper() for c in af.columns] 
      dfs.append(af)
    df = pd.concat(dfs)
    return df

  def __add_calculations(self, input_df, calculations):
    df = input_df.copy()
    error_strings = []
    for calc in calculations:
      df[calc['name']] = self.__apply_function(df, **calc)
      if calc.get('required_values') is not None:
        non_compliant = df.loc[[c not in calc['required_values'] for c in df[calc['name']]]]
        if len(non_compliant) > 0:
          error_locations = ', '.join([str(n+input['start']+1) for n in non_compliant.index.values])
          error_strings.append('Non-compliant values for [%s] found in the following rows: %s' % (calc['name'], error_locations))
    if error_strings:
      raise ValueError('\n'.join(error_strings))
    return df

  def __get_dataset_from_inputs(self, input_settings):
    df = None
    input_locations = self.__get_input_locations(input_settings)
    if input_locations:
      df = self.__get_dataset_from_input_locations(input_locations, input_settings['defaults'])
      if len(df) > 0:
        self.__update_functions(input_settings['functions'])
        df = self.__add_calculations(df, input_settings['calculations'])
      else: print('\nAll input files are empty.')
    else: print('No input files found.')
    return df

  def __update_dataset(self, settings):
    df = self.__get_dataset_from_inputs(settings['inputs'])
    output = self.__get_outputs_from_dataset(df, settings['outputs'])
    return output

  def run_update(self, settings_location):
    self.__update_start_time()
    run_settings = self.__get_settings(settings_location)
    output = [self.__update_dataset(s) for s in run_settings['etls']]
    return output