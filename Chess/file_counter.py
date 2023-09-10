import chess.pgn as pgn
import multiprocessing as mp

from pathlib import Path


def task(file_path):
    games_processed = 0
    with open(file_path) as f:
        while game:= pgn.read_game(f):
            games_processed += 1
    return (1, games_processed)    



if __name__ == '__main__':
    data_folder, file_paths = Path('./Chess/data/'), []
    print('Creating Queue...')
    # add each pgn file path to file_paths for processing
    for file in data_folder.glob('**/*.pgn'):
        file_paths.append(file)

    file_count, games, files_processed = len(file_paths), 0, 0

    with mp.Pool() as pool:
        results = []
        for file in file_paths:
            r = pool.apply_async(task, (file,))
            results.append(r.get())
        while files_processed <= file_count:
            f = results.pop(-1)
            files_processed, games = files_processed + f[0], games + f[1]
            print(
                f'Files left to proces: {str(file_count-files_processed)} | '
                f'Complete: {str((round(files_processed/file_count, 5))*100)[:5]}% | '
                f'Game Count: {str(games)} ...\r', 
                flush=True, 
                end='\r'
            )
        print(
            f'Total Files: {file_count} | '
            f'Files Processed: {files_processed} | '
            f'Game Count: {games}'
        )
