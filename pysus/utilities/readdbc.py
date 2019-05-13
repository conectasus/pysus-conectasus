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

def read_dbc(filename, encoding='utf-8'):
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
    if filename[0:2].upper() == 'RD':
        if isinstance(filename, str):
            filename = filename.encode()
        with NamedTemporaryFile(delete=False) as tf:
            dbc2dbf(filename, tf.name.encode())
            dbf = Dbf5(tf.name, codec=encoding)
        df = dbf.to_dataframe()
        os.unlink(tf.name)
        return df


    else:
        pandas2ri.activate()
        function = '''
        library(devtools)
        library(read.dbc)
        df <- read.dbc("%s")
        ''' % filename
        if filename[0:2].upper()=='BI':
            function+='\ndf$CNS_PAC <- NULL'
        robjects.r(function)
        df = robjects.globalenv['df']
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


