u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
from tempfile import NamedTemporaryFile
from io import BytesIO
import pandas as pd
from dbfread import DBF
from simpledbf import Dbf5
from pysus.utilities._readdbc import ffi, lib
from rpy2 import robjects
from rpy2.robjects import r, pandas2ri
from rpy2.robjects.conversion import localconverter
import pyarrow as pa
import pyarrow.parquet as pq


def append_to_parquet_table(dataframe, filepath=None, writer=None):
    """Method writes/append dataframes in parquet format.

    This method is used to write pandas DataFrame as pyarrow Table in parquet format. If the methods is invoked
    with writer, it appends dataframe to the already written pyarrow table.

    :param dataframe: pd.DataFrame to be written in parquet format.
    :param filepath: target file location for parquet file.
    :param writer: ParquetWriter object to write pyarrow tables in parquet format.
    :return: ParquetWriter object. This can be passed in the subsequenct method calls to append DataFrame
        in the pyarrow Table
    """
    table = pa.Table.from_pandas(dataframe)
    if writer is None:
        writer = pq.ParquetWriter(filepath, table.schema)
    writer.write_table(table=table)
    return writer

def read_dbc(filename,cachefile:str=None, encoding='utf-8'):
    """
    Opens a DATASUS .dbc file and return its contents as a pandas
    Dataframe.
    :param filename: .dbc filename
    :param encoding: encoding of the data
    :return: Pandas Dataframe.
    """
    '''
    if isinstance(filename, str):
        filename = filename.encode()
    with NamedTemporaryFile(delete=False) as tf:
        dbc2dbf(filename, tf.name.encode())
        dbf = Dbf5(tf.name,codec=encoding)
    df = dbf.to_dataframe()
    return df
    os.unlink(tf.name)
    '''
    #if filename[0:2].upper() == 'RD':
    if isinstance(filename, str):
        filename = filename.encode()
    with NamedTemporaryFile(delete=False) as tf:
        dbc2dbf(filename, tf.name.encode())
        dbf = DBF(tf.name, encoding=encoding)
    lista=[]
    writer = None
    for record in dbf:
        lista.append(dict(record))
        if(len(lista)>150000):
            df = pd.DataFrame(lista)
            lista = []
            writer = append_to_parquet_table(df, cachefile, writer)
    if writer:
        df = pd.DataFrame(lista)
        lista = None
        writer = append_to_parquet_table(df, cachefile, writer)
        writer.close()
        df = pd.read_parquet(cachefile)
    else:
        df = pd.DataFrame(lista)
    dbf = None
    del dbf
    os.unlink(tf.name)
    return df




def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new('char[]', os.path.abspath(infile))
    q = ffi.new('char[]', os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])


