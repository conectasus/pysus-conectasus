u"""
Downloads SIA data from Datasus FTP server
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""

import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc
from dbfread import DBF
import pandas as pd
from pysus.online_data import CACHEPATH

def create_parquet(state: str, year: int, month: int,tipo_dado: str ,cache: bool =False):
    """
    Download SIH records for state year and month and create cache 
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    """
    state = state.upper()
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    if year < 1992:
        raise ValueError("SIH does not contain data before 1994")
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    if year < 2008 and year > 1994:
        ftype = 'DBC'
        ftp.cwd('/dissemin/publicos/SIASUS/199407_200712/Dados')
        fname = 'PA{}{}{}.dbc'.format(state, year2, month)
        fname2 = None
    if year >= 2008:
        ftype = 'DBC'
        ftp.cwd('/dissemin/publicos/SIASUS/200801_/Dados'.format(year))
        fname = 'PA{}{}{}.dbc'.format(state, str(year2).zfill(2), month)
        fname2 = 'BI{}{}{}.dbc'.format(state, str(year2).zfill(2), month)
    # Check in Cache
    cachefile = os.path.join('/dados/SIA',tipo_dado,  'SIA_' + fname.split('.')[0] + '_.parquet')
    cachefile2 = os.path.join('/dados/SIA',tipo_dado, 'SIA_' + fname2.split('.')[0] + '_.parquet')
    if not cache:
        if tipo_dado=='PA':
            df = _fetch_file(fname, ftp, ftype,cachefile)
            df.to_parquet(cachefile)
            df = None
        if tipo_dado=='BI':
            df2 = _fetch_file(fname2, ftp, ftype,cachefile2)
            df2.to_parquet(cachefile2)
            df2 = None
    else:
        if not os.path.exists(cachefile):
            df = _fetch_file(fname, ftp, ftype)
            df.to_parquet(cachefile)
            df = None
        if not os.path.exists(cachefile2):
            df2 = _fetch_file(fname2, ftp, ftype)
            df2.to_parquet(cachefile2)
            df2 = None

def download(state: str, year: int, month: int,tipo_dado: str, cache: bool =True) -> object:
    """
    Download SIH records for state year and month and returns dataframe
    :param month: 1 to 12
    :param state: 2 letter state code
    :param year: 4 digit integer
    """
    state = state.upper()
    year2 = str(year)[-2:]
    month = str(month).zfill(2)
    if year < 1992:
        raise ValueError("SIH does not contain data before 1994")
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    if year < 2008 and year > 1994:
        ftype = 'DBC'
        ftp.cwd('/dissemin/publicos/SIASUS/199407_200712/Dados')
        fname = 'PA{}{}{}.dbc'.format(state, year2, month)
        fname2 = None
    if year >= 2008:
        ftype = 'DBC'
        ftp.cwd('/dissemin/publicos/SIASUS/200801_/Dados'.format(year))
        fname = 'PA{}{}{}.dbc'.format(state, str(year2).zfill(2), month)
        fname2 = 'BI{}{}{}.dbc'.format(state, str(year2).zfill(2), month)
    # Check in Cache
    cachefile = os.path.join('/dados/SIA',tipo_dado, 'SIA_' + fname.split('.')[0] + '_.parquet')
    cachefile2 = os.path.join('/dados/SIA',tipo_dado, 'SIA_' + fname2.split('.')[0] + '_.parquet')
    if not cache:
        if tipo_dado=='PA':
            df = _fetch_file(fname, ftp, ftype,cachefile)
            df.to_parquet(cachefile)
            return df
        if tipo_dado=='BI':
            df2 = _fetch_file(fname2, ftp, ftype,cachefile2)
            df2.to_parquet(cachefile2)
            return df2
    else:
        if not os.path.exists(cachefile):
            df = _fetch_file(fname, ftp, ftype)
            df.to_parquet(cachefile)
            return df
        else:
            df = pd.read_parquet(cachefile)
            return df
        if not os.path.exists(cachefile2):
            df2 = _fetch_file(fname2, ftp, ftype)
            df2.to_parquet(cachefile2)
            return df2
        else:
            df2 = pd.read_parquet(cachefile2)
            return df2


def _fetch_file(fname, ftp, ftype,cachefile:str=None):
    """
    Does the FTP fetching.
    :param fname: file name
    :param ftp: ftp connection object
    :param ftype: file type: DBF|DBC
    :return: pandas dataframe
    """
    print("Downloading {}...".format(fname))
    try:
        ftp.retrbinary('RETR {}'.format(fname), open(fname, 'wb').write)
    except:
        try:
            ftp.retrbinary('RETR {}'.format(fname.lower()), open(fname, 'wb').write)
        except:
            raise Exception("File {} not available".format(fname))
    if ftype == 'DBC':
        df = read_dbc(fname,cachefile, encoding='iso-8859-1', )
    elif ftype == 'DBF':
        dbf = DBF(fname, encoding='iso-8859-1')
        df = pd.DataFrame(list(dbf))
    os.unlink(fname)
    return df


