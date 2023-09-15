import os
import queue
import threading

from chessdotcom import (
    Client,
    get_player_games_by_month_pgn,
    get_player_game_archives,
    get_titled_players
)


def extraction():
    Client.request_config["headers"]["User-Agent"] = (
        "CS student research regarding the application of RNNs, Transformers, and Search"
        "Contact me at hickeys@iu.edu"
    )
    Client.rate_limit_handler.tries = 2
    Client.rate_limit_handler.tts = 4

    thread_count = 4
    q = queue.Queue()

    usernames = []
    for title in ['GM', 'IM', 'CM', 'FM', 'WIM', 'WGM', 'WCM', 'WFM']:
        usernames += get_titled_players(title).players

    # define what each thread will do
    def worker():
        while not q.empty():
            username = q.get()
            directory = f'./Chess/data/{username}'
            if not os.path.exists(directory):
                os.mkdir(directory)
            try:
                for game in get_player_game_archives(username).json['archives']: 
                    game = game.split('/')
                    file = f'./Chess/data/{username}/{username}-{game[-2]}-{game[-1]}.pgn'
                    if os.path.exists(file):
                        continue
                    with open(file, 'w') as f:
                        print(
                            f'Total number of users in queue: {str(q.qsize())} | '
                            f'Complete: {str((1-round(q.qsize()/len(usernames), 5))*100)[:5]}% ...\r',
                            flush=True, 
                            end='\r'
                        )
                        f.write(
                            get_player_games_by_month_pgn(
                                username=username,
                                year=game[-2],
                                month=game[-1]
                            ).text
                        )
                        f.close()
            except Exception as e:
                print(e)
                continue
            q.task_done()


    def extract_all_last_month():
        pass

    
    # push each task to the queue
    for username in set(usernames):
        q.put(username)


    for i in range(thread_count):
        t = threading.Thread(target=worker, daemon=True)
        t.start()

    q.join() # this will block the main thread from exiting until the queue is empty and all data has been processed
            
    print('done')

extraction()