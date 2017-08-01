import time

def exec_time(func):
    """Decorator to compute execution time of a function."""

    def wrapper(arg):
        start_time = time.time()
        res = func(arg)
        duration = time.time() - start_time
        print('{0} seconds'.format(duration))
        return 'eee'
    return wrapper

def sleeper(time_):

    def decorator(func):

        def wrapper(*args, **kargs):
            time.sleep(time_)
            func(*args, **kargs)

        return wrapper
    return decorator
