import chess.pgn as pgn
import multiprocessing as mp

from pathlib import Path


def count_games(file_path) -> int:
    game_counter: int = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            while game:= pgn.read_game(f):
                game_counter += 1
        except Exception as e:
            print(f'Error reading file: {file_path} | Error: {e}')
    return game_counter  

def main(path_to_PGNs:str) -> None:
    '''
        Creates a list of all pgn files to process from the path_to_PGNs.
        Distribues the files to worker cores/nodes which will count_games.

        Paramters:
            path_to_PGNs: str - path to directory containing the pgn files

        Returns:
            None
    '''
    data_folder, file_paths = Path(path_to_PGNs), []
    print('Creating Queue ...')
    for file in data_folder.glob('**/*.pgn'):
        file_paths.append(file)

    print('Creating worker pool ...')
    with mp.Pool() as pool:
        file_count, games, files_processed = len(file_paths), 0, 0
        results = pool.imap_unordered(count_games, file_paths)
        for game_count in results:
            files_processed, games = files_processed + 1, games + game_count
            if files_processed % 100 == 0:
                print(
                    f'Files in queue: {str(file_count-files_processed)} | '
                    f'Game Count: {str(games)} | Complete: '
                    f'{str((round(files_processed/file_count, 5)) * 100)[:5]}% \r', 
                    flush=True, 
                    end='\r'
                )
        print(
            f'Total Files: {file_count} | '
            f'Files Processed: {files_processed} | '
            f'Game Count: {games}'
        )

if __name__ == '__main__':
    main('./data')
