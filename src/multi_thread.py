from multiprocessing import Process

def multi_thread(fs: list) -> None:
    processes = []
    for f in fs:
        processes.append(Process(target=f))
    for t in processes:
        t.start()
    for t in processes:
        t.join()
        print(f"{t.name} is done.")

