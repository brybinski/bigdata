from itertools import repeat
import pandas as pd
from datetime import datetime
from filesplit.split import Split
from multiprocessing import Pool
import os


def count_time(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        print(f"Czas wczytywania {func.__name__}: {datetime.now() - start} sekund")
        return func(*args, **kwargs)
    return wrapper

def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def describe_size(dataframe: pd.DataFrame):
    output = ''
    tmp = str(sizeof_fmt(sum(dataframe.memory_usage(deep=True))))
    print(tmp)
    output += tmp +'\n'
    for i in dataframe.columns:
        tmp = sizeof_fmt(dataframe[i].memory_usage(deep=True))
        print(i, tmp)
        output += i + ' ' + tmp +'\n'

    return output

def apply_args_and_kwargs(func, args, kwargs):
    return func(*args, **kwargs)


def starmap_with_kwargs(pool, func, args_iter, kwargs_iter):
    args_for_starmap = zip(repeat(func), args_iter, kwargs_iter)
    return pool.starmap(apply_args_and_kwargs, args_for_starmap)


def split_file(filepath, chunksize, destination):
    split = Split(filepath, destination)
    split.bylinecount(linecount=chunksize, includeheader=True)


@count_time
def load_files(directory):

    files = [[f"{directory}/{f}"] for f in os.listdir(directory) if f.endswith(".csv")]

    kwargs_list = [
        {
            'on_bad_lines': "skip",
        }
        for n in range(len(files))
    ]

    pool = Pool(processes=16)
    args_iter = files

    results = starmap_with_kwargs(pool, pd.read_csv, args_iter, kwargs_list)
    results = pd.concat(results)

    return results

if __name__ == '__main__':
    split_file('/home/ryba/Documents/Code/snek/bigdata/lab1/csv_simple.csv', 1_000_000, '/home/ryba/Documents/Code/snek/bigdata/lab1/csv_chunks')
    df4 = load_files('/home/ryba/Documents/Code/snek/bigdata/lab1/csv_chunks')