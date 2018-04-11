
from __future__ import absolute_import

import argparse, re
import csv, os, json
import logging
import apache_beam as beam

from datetime import datetime
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


FOLDER = "result-folder"
ORIGIN_FOLDER = "folder"
FOLDER_CONTENT = []

def get_directory_contents(sPath):
    for sChild in os.listdir(sPath):                
        sChildPath = os.path.join(sPath,sChild)
        if os.path.isdir(sChildPath):
            get_directory_contents(sChildPath)
        else:
            FOLDER_CONTENT.append(sChildPath)


def get_iso_time(time_string, check_string):
  time = datetime.strptime(time_string, check_string).isoformat()
  return time


def time_parser(log):
  split_log = log.split(",")
  now = datetime.now()
  fyt = "%y%m%d%H%M%S"
  fYt = "%Y%m%d%H%M%S"
  fYt1 = "%Y-%m-%d%H:%M:%S"
  fYt2 = "%Y-%m-%d-%H.%M.%S"
  
  refy = "^("+str(now.year)[2:]+"\d{10}|"+str(now.year)[2:]+"\d{10})"
  refY = "^("+str(now.year)+"\d{10}|"+str(now.year-1)+"\d{10})"
  refY1 = "\w{4}-\w{2}-\w{4}:\w{2}:\w{2}"
  refY2 = str(now.year)+"-\w{2}-\w{2}-\w{2}.\w{2}.\w{2}|"+str(now.year-1)+"-\w{2}-\w{2}-\w{2}.\w{2}.\w{2}"
  
  for index, value in enumerate(split_log):
    if not value:
      del split_log[index]
    result = None
    try:
      match = re.findall(refY1, value)
      if match:
        result = get_iso_time(match[0], fYt1)

    except Exception as e:
      print(e)
    try:
      match = re.findall(refY, value)
      if match:
        result = datetime.strptime(match[0],fYt).isoformat()
    except Exception as e:
      print(e)
    try:
      match = re.findall(refy, value)
      if match:
        result = datetime.strptime(match[0],fyt).isoformat()
    except Exception as e:
      print(e)
    try:
      match = re.findall(refY2, value)
      if match:
        result = datetime.strptime(match[0],fYt2).isoformat()
    except Exception as e:
      print(e)
    if result:
      split_log[index] = result
    log = ",".join(split_log)
  return log


class ProcessTransformLog(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.
  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time
  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
  The human-readable time string is not used here.
  """
  def __init__(self):
    super(ProcessTransformLog, self).__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')
    self.data = list()
    self.header = list()
  def process(self, elem):
    try:
        elem_encode = elem.encode()
        elem_parse = elem_encode.split(",")
        elem_parse = [elem for elem in elem_parse]

        output = time_parser(elem_encode)
        yield output
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)


class TransformLog(beam.PTransform):
  """change format cirurgias"""

  def expand(self, pcoll):
    return (
        pcoll
        | 'ProcessTransformLog' >> beam.ParDo(ProcessTransformLog()))

class JoinTable(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.
  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time
  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
  The human-readable time string is not used here.
  """
  def __init__(self):
    super(JoinTable, self).__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
        if len(elem[1]["SEXO"]) == 0:
            elem[1]["SEXO"][0] = " "
        else:
            elem[1]["SEXO"] = elem[1]["SEXO"][0]
        yield {
            "DOENTE": elem[0],
            "SEXO": elem[1]["SEXO"],
            "SERVICO": elem[1]["SERVICO"][0]
        }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)



class JoinRow(beam.PTransform):
  """JOIN SERVICO and DOENTE tables"""

  def expand(self, pcoll):
    return (
        pcoll
        | 'JoinTable' >> beam.ParDo(JoinTable()))


class WriteToJsonLines(beam.PTransform):
    def __init__(self, file_name):
        self._file_name = file_name

    def expand(self, pcoll):
        return (pcoll
                | 'write to text' >> beam.io.WriteToText("{}/{}".format(FOLDER,self._file_name), file_name_suffix='.csv'))

# [START]
def run(argv=None):
  """Main entry point; defines and runs the user_score pipeline."""
  parser = argparse.ArgumentParser()
  get_directory_contents(ORIGIN_FOLDER)

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.

  # parser.add_argument('--output',
  #                     type=str,
  #                     required=True,
  #                     help='Path to the output file(s).')
  # parser.add_argument('--dataset',
  #                     type=str,
  #                     required=True,
  #                     help='BigQuery Dataset to write tables to. '
  #                     'Must already exist.')

  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  if options.view_as(GoogleCloudOptions).project is None:
    parser.print_usage()
    # print(sys.argv[0] + ': error: argument --project is required')
    # sys.exit(1)
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  for file in FOLDER_CONTENT:
    file_name = file.split("/")[-1]

    file_csv = p | file_name >> beam.io.ReadFromText(file)
    
    file_lines = (file_csv
       | "{}".format(file_name+"transform") >> TransformLog()
    )

    file_lines | "{}".format(file_name+"write") >> WriteToJsonLines("{}".format(file_name))
    
    # write_internamento = (file_lines
    #    | 'WriteInternamentoBigTable' >> WriteToPostgres(
    #     file_name.upper())
    # )
  # internamento_csv = p | 'ReadInputInternamentoText' >> beam.io.ReadFromText(INTERNAMENTO_PROD_TABLE)
  # second_log_csv = p | 'ReadInputTransformLogText' >> beam.io.ReadFromText(FOLDER_CONTENT)
  # first_log_csv = p | 'ReadInputFirstLogText' >> beam.io.ReadFromText(FIRST_TABLE)
  # internamento_lines = (internamento_csv
  #    | 'Internamento' >> Internamento()
  # )
  # second_log_lines = (second_log_csv
  #      | 'TransformLog' >> TransformLog()
  # )
  # first_log_lines = (first_log_csv
  #      | 'FirstLog' >> FirstLog()
  # )
  #
  # cirurgia_filtered = (cirurgias_lines
  #      | 'filter_cirurgia' >> beam.Map(
  #       lambda x: tuple((x['DOENTE'].strip(), x['SERVICO'].strip())))
  # )
  #
  # doente_filtered = (first_log_lines
  #      | 'filter_doente' >> beam.Map(
  #       lambda x: tuple((x['DOENTE'].strip(), x['SEXO'].strip())))
  # )

  # join_table = ({"SERVICO":cirurgia_filtered,"SEXO":doente_filtered}
  #      | 'join_rows' >> beam.CoGroupByKey()
  #      | 'join_table' >> JoinRow()
  # )

  # write_internamento = (internamento_lines
  #   | 'WriteInternamentoBigTable' >> WriteToBigQuery(
  #       OUTPUT_BQ_INTERNAMENTO_PROD_TABLE, args.dataset, {
  #       "DOENTE": "STRING",
  #       "T_DOENTE": "STRING",
  #       "DIFF": "INTEGER"})
  #   )
  #
  # write_cirurgias = (cirurgias_lines
  #   | 'WriteCirurgiasBigTable' >> WriteToBigQuery(
  #       OUTPUT_BQ_CIRURGIAS_PROD_TABLE, args.dataset, {
  #       "DOENTE": "STRING",
  #       "T_DOENTE": "STRING",
  #       "SERVICO": "STRING",
  #       "FLAG_CIRURGIA": "STRING"})
  #  )
  #
  # write_doente = (doente_lines
  #   | 'WriteDoenteBigTable' >> WriteToBigQuery(
  #       OUTPUT_BQ_DOENTE_TABLE, args.dataset, {
  #       "DOENTE": "STRING",
  #       "T_DOENTE": "STRING",
  #       "CONCELHO": "STRING",
  #       "DT_NASC": "STRING",
  #       "SEXO": "STRING"})
  #   )
  # write_join = (join_table
  #   | 'WriteJoinBigTable' >> WriteToBigQuery(
  #       OUTPUT_BQ_JOIN, args.dataset, {
  #       "DOENTE": "STRING",
  #       "SEXO": "STRING",
  #       "SERVICO": "STRING"})
  #   )

  result = p.run()
  result.wait_until_finish()
# [END]


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
