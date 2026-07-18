"""Why QKData uses Queue.get(timeout) instead of micro-sleep polling.

This standalone demonstration does not connect to QUIK and does not benchmark
production latency. It simply shows a blocking queue consumer waking when data
arrives and remaining stoppable through a sentinel.
"""
from queue import Queue
from threading import Thread
from time import sleep


STOP = object()


def consumer(queue: Queue):
    while True:
        item = queue.get()
        if item is STOP:
            return
        print('received:', item)


def main():
    queue = Queue()
    thread = Thread(target=consumer, args=(queue,), daemon=True)
    thread.start()
    for bar_number in range(3):
        sleep(0.2)
        queue.put({'bar': bar_number})
    queue.put(STOP)
    thread.join(timeout=2.0)
    assert not thread.is_alive()


if __name__ == '__main__':
    main()
