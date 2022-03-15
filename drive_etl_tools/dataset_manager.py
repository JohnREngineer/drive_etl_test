from this import d
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
import pathlib
import sys
import itertools
import pprint

class DatasetManager:
  def __init__(self):
    self.upload = True
    self.verbose = False
    self.start_time_unix = ''
    self.etl_functions = {}
    self.folder_string = 'https://drive.google.com/drive/folders/%s'
    self.file_string = 'https://drive.google.com/file/d/%s/edit'
    self.sheet_string = 'https://docs.google.com/spreadsheets/d/%s/edit'
    self.__update_start_time()
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
    names, calculations, nick_names = list(map(list,list(zip(*columns))))
    nick_names = [p or n for n,p in zip(names,nick_names)]
    nf = df[calculations].copy()
    nf.columns = list(names)
    return nf, nick_names

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

  def __apply_function(self, df, get_df=False, name=None, inputs=None, function=None, args=None, kwargs=None):
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
    if not isinstance(input_columns, list) and (input_columns not in df.columns.values):
      print(df.columns.values)
    new_column = df[input_columns].apply(apply_function, **apply_kwargs)
    if get_df:
      df_new = df.copy()
      df_new[name] = new_column
      return df_new
    else:
      return new_column

  def __split_and_replace(self, string, split_chars = ['\n','?','('], replace_chars=[',']):
    out_string = string
    for s in split_chars:
      out_string = out_string.split(s)[0]
    for r in replace_chars:
      out_string = out_string.replace(r,'')
    return out_string

  def __load_json(self, path):
    with open(path, 'r') as f:
      return json.load(f)

  def __get_settings_from_key(self, key):
    key = self.__sanitize_key(key)
    path = self.__download_drive_file(key)
    settings = self.__load_json(path)
    print('\tLoaded settings from %s' % (self.file_string % key))
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
      raise ValueError('No settings found in folder %s' % (self.folder_string % key))
    else:
      print('\tFound '+first['title']+' in %s' % (self.folder_string % key))
    # Get settings
    settings = self.__get_settings_from_key(first.get('key'))
    return settings

  def __get_etl_settings_from_location(self, etl_settings_location):
    print('Settings:')
    settings_getters = {
        'object': lambda s: s['object'],
        'path': lambda s: self.__load_json(s['path']),
        'key': lambda s: self.__get_settings_from_key(s.get('key')),
        'folder': lambda s: self.__get_settings_from_folder(s.get('key'))
    }
    return settings_getters[etl_settings_location['type']](etl_settings_location)

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
    print('\tLoading %s' % (self.file_string % key))
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
    print('\tFound %s in %s' % (first['title'], self.folder_string % key))
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
    input_locations = [{'key': f.get('id')} for f in files if f.get('mimeType') == 'application/vnd.google-apps.spreadsheet']
    return input_locations

  def __get_input_locations(self, inputs):
    inputs_getters = {
        'list': lambda i: i['list'],
        'sheet': lambda i: self.__get_inputs_from_sheet(i['location'], **i['defaults']),
        'folder': lambda i: self.__get_inputs_from_folder(i['location']),
    }
    inputs_prints = {
        'list': lambda i:'Inputs passed directly.',
        'sheet': lambda i:'Inputs from sheet: %s' % (self.sheet_string % self.__sanitize_key(i['location']['key'])),
        'folder': lambda i:'Inputs from folder: %s' % (self.folder_string % self.__sanitize_key(i['location']['key'])),
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
    parent_sheet = None
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

  def __export_to_excel_from_template(self, df, path, template_location, nick_names=None):
    # self.pv('__export_to_excel_from_template:template_location %s'%template_location)
    template_path = self.__download_drive_file(self.__sanitize_key(template_location['key']))
    os.rename(template_path, path)
    sheet_name = template_location.get('sheet',0)
    if str(sheet_name).isnumeric():
        xl = pd.ExcelFile(path)
        sheet_name = xl.sheet_names[int(sheet_name)]
    ef = pd.read_excel(path, sheet_name)
    ef.columns = df.columns
    ef = ef.append(df, ignore_index=True)
    if nick_names:
      ef.columns = nick_names
    with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
      ef.to_excel(writer, sheet_name, index=False)
    print('\t\tCreated %s'%(path))

  def __upload_file_to_folder(self, path, folder=None):
    if folder and folder.get('key') and self.upload:
      sanitized_key = self.__sanitize_key(folder['key'])
      f = self.drive.CreateFile({'parents': [{'kind': 'drive#fileLink', 'id': sanitized_key}]})
      f.SetContentFile(path)
      f.Upload()
      print('\t\tUploaded %s to %s'%(path, self.folder_string % sanitized_key))

  def __append_to_parent_sheet(self, df, parent_sheet=None):
    if parent_sheet and self.upload:
      for _, row in df.iterrows():
        parent_sheet.append_rows(values=[list(row.values)])
      print('Appended new data to parent dataset.')
    
  def __get_sheet_output_from_dataframe(self, input_df, sheet_output_settings):
    # self.pv('__get_output_from_dataframe:output settings %s'%sheet_output_settings)
    df = self.__apply_filters(input_df, sheet_output_settings.get('filters'))
    df, nick_names = self.__get_output_from_columns(df, sheet_output_settings['columns'])
    df, parent_sheet = self.__deduplicate_dataset(df, sheet_output_settings.get('dedup_column'), sheet_output_settings.get('parent_dataset'))
    print('\tNew %s:\t%s' % (sheet_output_settings['sheet_name'], len(df)))
    path = None
    if (len(df) > 0):
      path = 'New_%s_%s.xlsx'%(sheet_output_settings['sheet_name'], self.start_time_unix)
      self.__append_to_parent_sheet(df, parent_sheet)
      self.__export_to_excel_from_template(df, path, sheet_output_settings.get('excel_template_location'), nick_names)
      self.__upload_file_to_folder(path, sheet_output_settings.get('export_folder'))
    return [df, path]

  def __get_output_dict_from_dataset(self, input_dataset, file_output_settings_list):
    # self.pv('__get_outputs_from_dataframe:output settings %s'%file_output_settings_list)
    if not input_dataset:
      return {}
    # if (input_df is None) or (len(input_df) == 0) :
    #   return self.__get_empty_output(len(file_output_settings_list))
    outputs = []
    # outputs_dict = {}
    for file_output_settings in file_output_settings_list:
        # dataset = {file_output_settings['sheet_output_settings_list'][0]['dataframe_name']:input_dataset}
        file_dict, df, path = self.__get_file_output_from_meta_dataframe(input_dataset, file_output_settings)
        outputs.append([df,path,file_dict])
        # for sheet_output_settings in file_output_settings['sheet_output_settings_list']:
        #   outputs.append(self.__get_sheet_output_from_dataframe(input_df, sheet_output_settings))
    transposed_outputs = list(map(list,list(zip(*outputs))))
    return transposed_outputs
    # return outputs_dict


  def __get_dataframe_from_input_locations(self, input_locations, defaults=None):
    dfs = []
    print('Inputs:')
    for location in input_locations:
      print('\t'+'https://docs.google.com/spreadsheets/d/'+location['key']+'/edit')
      full_location = defaults.copy()
      full_location.update(location)
      af = self.__get_df_from_drive(**full_location)[0]
      af.columns = [self.__split_and_replace(c).strip().upper() for c in af.columns] 
      dfs.append(af)
    df = pd.concat(dfs)
    return df

  def __add_calculations(self, input_df, calculations):
    df = input_df.copy()
    error_strings = []
    for calc in calculations:
      required_values = calc.pop('required_values', None) # We do this first to remove required_values from calc
      df[calc['name']] = self.__apply_function(df, **calc)
      if required_values is not None:
        non_compliant = df.loc[[c not in required_values for c in df[calc['name']]]]
        if len(non_compliant) > 0:
          error_locations = ', '.join([str(n+input['start']+1) for n in non_compliant.index.values])
          error_strings.append('Non-compliant values for [%s] found in the following rows: %s' % (calc['name'], error_locations))
    if error_strings:
      raise ValueError('\n'.join(error_strings))
    return df

  def __get_dataset_from_input_settings(self, input_settings):
    datasets = {}
    input_locations = self.__get_input_locations(input_settings)
    if input_locations:
      df = self.__get_dataframe_from_input_locations(input_locations, input_settings['defaults'])
      if len(df) > 0:
        df = self.__add_calculations(df, input_settings['calculations'])
        datasets.update({input_settings['dataset_output']['dataframe_name']:df})
      else: print('\nAll input files are empty.')
    else: print('No input files found.')
    return datasets

  def __run_etls(self, etl_settings):
    # self.pv(':getting %s'%etl_settings['dataset_input_settings'])
    dataset = self.__get_dataset_from_input_settings(etl_settings['dataset_input_settings'])
    self.pv('__run_etls:dataset',dataset)
    outputs_dict, dfs, paths = self.__get_output_dict_from_dataset(dataset, etl_settings['dataset_output_settings'])
    # output_dict = self.__get_outputs_dict_from_meta_dataframe_dict(dataset, etl_settings['dataset_output_settings'])
    # self.pv('__run_etls:output_dict',output_dict)
    result = {
      etl_settings['etl_name']: {
        'dataframe': dfs[0],
        'path': paths[0]
      }
    }
    self.pv(':result %s'%result)
    # return result
    return result

  def __create_dataset_from_meta_calculations(self, previous_results, dataset_input_settings):
    dataset = {}
    for input_settings in dataset_input_settings:
      previous_df = previous_results.get(input_settings['dataframe_name']).get('dataframe')
      df = self.__add_calculations(previous_df, input_settings['calculations'])
      dataset.update({input_settings['dataframe_name']: df})
    return dataset

  def __get_output_dataframe_from_dataset(self, dataset, output):
    return dataset[output['dataframe_name']]

  def __get_dataframe_dict_from_previous_results(self, previous_results, etl_input_settings):
    dataframe_dict = {}
    for dataset_settings in etl_input_settings:
      dataset = self.__create_dataset_from_meta_calculations(previous_results, dataset_settings['dataset_input_settings_list'])
      df = self.__get_output_dataframe_from_dataset(dataset, dataset_settings['dataset_output'])
      result = { dataset_settings['name']: df}
      dataframe_dict.update(result)
    return dataframe_dict

  def __get_sheet_output_from_meta_dataframe(self, input_df, path, sheet_output_settings):
    df = self.__apply_filters(input_df, sheet_output_settings.get('filters'))
    df, nick_names = self.__get_output_from_columns(df, sheet_output_settings['columns'])
    df, parent_sheet = self.__deduplicate_dataset(df, sheet_output_settings.get('dedup_column'), sheet_output_settings.get('parent_dataset'))
    print('\tNew %s:\t%s' % (sheet_output_settings['sheet_name'], len(df)))
    if (len(df) == 0):
      return {}
    df.columns = nick_names
    self.__append_to_parent_sheet(df, parent_sheet)
    sheet_name = sheet_output_settings.get('sheet_name',0)
    if str(sheet_name).isnumeric():
        xl = pd.ExcelFile(path)
        sheet_name = xl.sheet_names[int(sheet_name)]
    with pd.ExcelWriter(path,  engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
      df.to_excel(writer, sheet_name, index=False)
    return {sheet_output_settings['sheet_name']: df}, df    

  def __get_file_output_from_meta_dataframe(self, dataframes, file_output_settings):
    path = 'New_%s_%s.xlsx'%(file_output_settings['file_name'], self.start_time_unix)
    template_path = self.__download_drive_file(self.__sanitize_key(file_output_settings['excel_template_location']['key']))
    os.rename(template_path, path)
    sheet_dataframes_dict = {}
    dfs = []
    for sheet_output_settings in file_output_settings['sheet_output_settings_list']:
      self.pv('__get_file_output_from_meta_dataframe:sheet_output_settings',sheet_output_settings)
      input_df = dataframes.get(sheet_output_settings['dataframe_name'])
      self.pv('__get_file_output_from_meta_dataframe:input_df',input_df)
      df_dict, df = self.__get_sheet_output_from_meta_dataframe(input_df, path, sheet_output_settings)
      sheet_dataframes_dict.update(df_dict)
      dfs.append(df)
    if sheet_dataframes_dict:
      self.__upload_file_to_folder(path, file_output_settings.get('export_folder'))
    file_dict = {file_output_settings['file_name']: sheet_dataframes_dict}
    return [file_dict, dfs, path]

  def __get_outputs_dict_from_meta_dataframe_dict(self, dataframes, file_output_settings_list):
    outputs_dict = {}
    for file_output_settings in file_output_settings_list:
      file_dict, dfs, path = self.__get_file_output_from_meta_dataframe(dataframes, file_output_settings)
      output = {
        file_output_settings['file_name']: {
          'file_dict': file_dict,
          'dataframes': dfs,
          'path': path,
        }
      }
      outputs_dict.update(output)
    return outputs_dict

  # def __get_dataset_from_input_settings(self, input_settings):
  #   self.__get_dataframe_from_input_settings(input_settings)
  #   df = None
  #   input_locations = self.__get_input_locations(input_settings)
  #   if input_locations:
  #     df = self.__get_dataframe_from_input_locations(input_locations, input_settings['defaults'])
  #     if len(df) > 0:
  #       df = self.__add_calculations(df, input_settings['calculations'])
  #     else: print('\nAll input files are empty.')
  #   else: print('No input files found.')
  #   dataset = {
  #     'name': df
  #   }
  #   return df

  def __run_meta_etls(self, previous_results, etl_settings):
    dataframe_dict = self.__get_dataframe_dict_from_previous_results(previous_results, etl_settings['dataset_input_settings'])
    outputs_dict = self.__get_outputs_dict_from_meta_dataframe_dict(dataframe_dict, etl_settings['dataset_output_settings'])
    return outputs_dict

  def pv(self, text=None, object=None):
    if self.verbose:
      if text is not None:
        print(text)
      if object is not None:
        pprint.pprint(object)

  def run_ETLs(self, etl_settings_location):
    self.upload = False
    etl_settings = self.__get_etl_settings_from_location(etl_settings_location)
    self.__update_functions(etl_settings['functions'])
    self.verbose = True
    results_list = [self.__run_etls(s) for s in etl_settings['etls']]
    results = {}
    for r in results_list:
      results.update(r)
    self.upload=True
    self.pv('run_ETLs:results',results)
    meta_outputs_dict = [self.__run_meta_etls(results, s) for s in etl_settings['meta_etls']]
    return meta_outputs_dict
    transposed_output = list(map(list,list(zip(*results)))) 
    dfs, paths = [list(itertools.chain(*o)) for o in transposed_output]
    paths = [p for p in paths if p]
    return dfs, paths