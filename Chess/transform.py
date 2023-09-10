import chess.pgn as pgn
import multiprocessing as mp
import pandas as pd
import sqlalchemy

from pathlib import Path
from sqlalchemy import MetaData
from sqlalchemy.pool import NullPool
from time import sleep


def transform(file_path) -> pd.DataFrame or None:
    games = []
    with open(file_path) as f:
        while game:= pgn.read_game(f):
            if game:
                headers = {k.upper():v for k, v in list(game.headers.items())}
                games.append({
                    'EVENT':            headers.get('EVENT', None),
                    'SITE':             headers.get('SITE', None),
                    'START_DATE':       headers.get('DATE', None),
                    'ROUND':            headers.get('ROUND', None),
                    'WHITE':            headers.get('WHITE', None),
                    'BLACK':            headers.get('BLACK', None),
                    'RESULT':           headers.get('RESULT', None),
                    'CURRENT_POSITION': headers.get('CURRENTPOSITION', None),
                    'TIMEZONE':         headers.get('TIMEZONE', None),
                    'ECO':              headers.get('ECO', None),
                    'ECO_URL':          headers.get('ECOURL', None),
                    'UTC_DATE':         headers.get('UTCDATE', None),
                    'UTC_TIME':         headers.get('UTCTIME', None),
                    'WHITE_ELO':        headers.get('WHITEELO', None),
                    'BLACK_ELO':        headers.get('BLACKELO', None),
                    'TIME_CONTROL':     headers.get('TIMECONTROL', None),
                    'TERMINATION':      headers.get('TERMINATION', None),
                    'START_TIME':       headers.get('STARTTIME', None),
                    'END_DATE':         headers.get('ENDDATE', None),
                    'END_TIME':         headers.get('ENDTIME', None),
                    'LINK':             headers.get('LINK', None),
                    'MAINLINE':         str(game.mainline()),
                    'TOURNAMENT':       headers.get('TOURNAMENT', None),
                    'VARIANT':          headers.get('VARIANT', None),
                    'FEN':              headers.get('FEN', None),
                    'SETUP':            headers.get('SETUP', None),
                    'MATCH':            headers.get('MATCH', None)
                })
        f.close()
    if len(games) < 1:
        return None
    result = pd.DataFrame.from_dict(games)
    for attr in ['START', 'END', 'UTC']:  # cast dates and times accordingly
        date, time = attr + '_DATE', attr + '_TIME'
        result[date] = pd.to_datetime(result[date], format='%Y.%m.%d').dt.date
        result[time] = pd.to_datetime(result[time], format='%H:%M:%S').dt.time
    sleep(.1)
    return result


engine = sqlalchemy.create_engine(
    'mariadb+pymysql://{user}:{password}@{server}:{port}/{database}' \
        .format(
            user='admin'
            ,password='C0lumnStore!'
            ,server='localhost'
            ,port='3307'
            ,database='chess_analytics'
        ),
    poolclass=NullPool
)


if __name__ == '__main__':
    print('Retrieving pgn files ...')
    data_folder, file_paths = Path('./Chess/data/'), []
    for file in data_folder.glob('**/*.pgn'):
        file_paths.append(file)

    print('Creating worker pool ...')
    with mp.Pool() as pool:
        file_count, games, files_processed = len(file_paths), 0, 0
        results = pool.imap_unordered(transform, file_paths)
        batch = []
        print('Will update after every 10K files are processed ...')
        for df in results:
            if df is not None:
                games += df.shape[0]
                batch += df.to_dict('records')
            if len(batch) > 1000:
                try:
                    conn = engine.connect()
                    meta = MetaData()
                    meta.reflect(bind=engine)
                    table = meta.tables['staging']
                    conn.begin()
                    conn.execute(table.insert(), batch)
                    conn.commit()
                    batch = []
                except Exception as e:
                    conn.rollback()
                    print(e)
                finally:
                    conn.close()
            if files_processed % 10000 == 0:
                print(
                    f'Files left to proces: {str(file_count-files_processed)} | '
                    f'Complete: {str((round(files_processed/file_count, 5))*100)[:5]}% | '
                    f'Game Count: {str(games)} ...\r', 
                    flush=True, 
                    end='\r'
                )
            files_processed += 1
        print(
            f'Total Files: {file_count} | '
            f'Files Processed: {files_processed} | '
            f'Game: {games}'
        )