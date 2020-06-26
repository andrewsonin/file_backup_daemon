#!/usr/bin/env python
from argparse import ArgumentParser
from collections import defaultdict
from functools import partial
from os import listdir, walk, chdir, makedirs, devnull, PathLike
from os.path import isfile, getmtime, abspath, splitext, getsize, join as join_path
from shutil import copy2
from time import sleep, time
from typing import Union, NoReturn, Callable, Iterator, Generator, Dict


def rec_file_iter(backup_dir: Union[PathLike, str]) -> Generator[str, None, None]:
    """Yields files from the ``cwd`` recursively.
    Creates the same non-empty directory structure in 'backup_dir' as in ``cwd``.

    Args:
        backup_dir:  folder to save backups to
    Yields:
        relative paths to files
    """
    for (folder_name, _, file_names) in walk('.'):
        if file_names:
            dir_to_save = join_path(backup_dir, folder_name)
            makedirs(dir_to_save, exist_ok=True)
            for file_name in file_names:
                yield join_path(folder_name[2:], file_name)


def main(dir_to_monitor: Union[PathLike, str],
         backup_dir: Union[PathLike, str],
         refresh_rate: Union[int, float] = 1,
         logfile: Union[PathLike, str] = devnull,
         recursive: bool = False) -> NoReturn:
    """Tracks changes to files inside 'dir_to_monitor' and creates their backups.

    Args:
        dir_to_monitor:  tracking directory
        backup_dir:      absolute path to backup folder
        refresh_rate:    time in seconds after which the information about the tracked files is being updated
        logfile:         logfile
        recursive:       track files inside child directories
    Returns:
        NoReturn. Stop using KeyboardInterrupt is expected
    """
    backup_dir = abspath(backup_dir)
    _bdir_len_p1 = len(backup_dir) + 1

    makedirs(backup_dir, exist_ok=True)
    file_change_time: Dict[str, Union[int, float]] = defaultdict(int)
    if recursive:
        file_getter: Callable[[], Iterator[str]] = partial(rec_file_iter, backup_dir)
    else:
        def file_getter():
            return filter(isfile, listdir())

    with open(logfile, 'a') as log:
        if getsize(logfile):
            log.write('\n')
        log.write(f'Source[{abspath(dir_to_monitor)}]\tDestination[{backup_dir}]\n')
        chdir(dir_to_monitor)
        while True:
            for file_name in file_getter():
                time_change = getmtime(file_name)
                if time_change > file_change_time[file_name]:
                    stem, extension = splitext(file_name)
                    backup_name = join_path(
                        backup_dir,
                        f'{stem}_{time_change:.0f}{extension}'
                    )
                    time_stamp = time()
                    copy2(file_name, backup_name)
                    file_change_time[file_name] = time_stamp
                    log.write(f'{file_name}\t{backup_name[_bdir_len_p1:]}\n')
            sleep(refresh_rate)


if __name__ == '__main__':
    parser = ArgumentParser(description='File backup daemon')
    parser.add_argument(
        'dir_to_monitor', metavar='dir_to_monitor', type=str,
        help='Directory with files to be backed up'
    )
    parser.add_argument(
        'backup_dir', metavar='backup_dir', type=str,
        help='Backup directory'
    )
    parser.add_argument(
        '-t', metavar='(FLOAT | INT)', dest='refresh_rate', type=float, default=1,
        help='Refresh rate of the recent file modification time (in seconds)'
    )
    parser.add_argument(
        '-o', metavar='FILE', dest='log_file', type=str, default=devnull,
        help='Path to logfile'
    )
    parser.add_argument(
        '--recursive', action='store_true', dest='recursive',
        help='Track files inside child directories as well'
    )
    args = parser.parse_args()

    dir_to_monitor = args.dir_to_monitor
    log_file = args.log_file

    refresh_rate = args.refresh_rate
    backup_dir = args.backup_dir
    recursive = args.recursive
    del parser, args

    main(dir_to_monitor, backup_dir, refresh_rate, log_file, recursive)
