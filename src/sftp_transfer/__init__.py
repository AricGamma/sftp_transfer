
import argparse
import logging
import os

import paramiko
from tqdm import tqdm

from sftp_transfer.logger import get_logger
from sftp_transfer.pipe_process import PipeProcessor


logger = get_logger(__name__, level=logging.DEBUG if "DEBUG" in os.environ else logging.INFO)

def build_args():
    parser = argparse.ArgumentParser(description="SFTP Transfer")
    parser.add_argument("-sh", "--source-host", type=str, help="Source host")
    parser.add_argument("-sp", "--source-port", type=int, default=22, help="Source port")
    parser.add_argument("-su", "--source-user", type=str, help="Source user")
    parser.add_argument("-spw", "--source-password", type=str, help="Source password")

    parser.add_argument("-dh", "--dest-host", type=str, help="Target host")
    parser.add_argument("-dp", "--dest-port", type=int, default=22, help="Target port")
    parser.add_argument("-du", "--dest-user", type=str, help="Target user")
    parser.add_argument("-dpw", "--dest-password", type=str, help="Target password")
    parser.add_argument("-sf", "--source-path", required=True, type=str, help="Source path")
    parser.add_argument("-df", "--dest-path", required=True, type=str, help="Target path")
    parser.add_argument("--num-workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=1)
    args = parser.parse_args()
    return args

def get_local_mock_sftp_connection():
    class MockSFTPAttributes:
        def __init__(self, entry: os.DirEntry):
            self.name = entry.name
            self.st_mode = entry.stat().st_mode
            self.path = entry.path

        @property
        def filename(self):
            return self.name
        
        def __str__(self):
            return self.path

    class MockSFTP:
        def stat(self, path):
            return os.stat(path)
            
        def open(self, path, mode='r', *args, **kwargs):
            return open(path, mode, *args, **kwargs)
            
        def mkdir(self, path):
            return os.mkdir(path)
            
        def listdir_attr(self, path):
            return map(MockSFTPAttributes, os.scandir(path))
            
        def close(self):
            pass
            
    return MockSFTP()

def get_sftp_connection(host, port, username, password):
    if not host or port is None or username is None:
        return get_local_mock_sftp_connection()
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


def sync_file(tasks, args):
    source_host = args.source_host
    dest_host = args.dest_host
    source_port = args.source_port
    dest_port = args.dest_port
    source_username = args.source_user
    dest_username = args.dest_user
    source_password = args.source_password
    dest_password = args.dest_password

    source_sftp = get_sftp_connection(source_host, source_port, source_username, source_password)
    dest_sftp = get_sftp_connection(dest_host, dest_port, dest_username, dest_password)
    if not isinstance(tasks, list):
        tasks = [tasks]

    try:
        for source_path, dest_path in tasks:
            try:
                source_stat = source_sftp.stat(source_path)
                try:
                    dest_stat = dest_sftp.stat(dest_path)
                    if dest_stat.st_size == source_stat.st_size:
                        logger.debug(f"File {source_path} already synced.")
                        return True
                    start_byte = dest_stat.st_size
                except FileNotFoundError:
                    start_byte = 0

                total_size = source_stat.st_size - start_byte
                with source_sftp.open(source_path, 'rb') as source_file:
                    source_file.seek(start_byte)
                    with dest_sftp.open(dest_path, 'ab') as dest_file:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Syncing {os.path.basename(source_path)}") as pbar:
                            while True:
                                data = source_file.read(4096)
                                if not data:
                                    break
                                dest_file.write(data)
                                pbar.update(len(data))
            except Exception as e:
                logger.error(f"Error syncing file {source_path}: {e}")
                logger.exception(e)
    except Exception as ex:
        logger.error(f"Error syncing files: {ex}")
        logger.exception(ex)
    finally:
        source_sftp.close()
        dest_sftp.close()

def traverse(args):
    source_host = args.source_host
    dest_host = args.dest_host
    source_port = args.source_port
    dest_port = args.dest_port
    source_username = args.source_user
    dest_username = args.dest_user
    source_password = args.source_password
    dest_password = args.dest_password
    source_path = args.source_path
    dest_path = args.dest_path

    source_sftp = get_sftp_connection(source_host, source_port, source_username, source_password)
    dest_sftp = get_sftp_connection(dest_host, dest_port, dest_username, dest_password)

    stack = []
    stack.append((source_path, dest_path))

    pbar = tqdm(desc="Traversing directory")
    try:
        while len(stack) > 0:
            s, d = stack.pop()
            s_stat = source_sftp.stat(s)
            if s_stat.st_mode & 0o40000:
                # Directory
                try:
                    dest_sftp.stat(d)
                except FileNotFoundError:
                    dest_sftp.mkdir(d)
                for item in source_sftp.listdir_attr(s):
                    s_item = os.path.join(s, item.filename)
                    d_item = os.path.join(d, item.filename)
                    if item.st_mode & 0o40000:
                        stack.append((s_item, d_item))
                    else:
                        pbar.update(1)
                        yield s_item, d_item
            else:
                yield s, d
    except Exception as e:
        logger.error(f"Error traversing directory {s}: {e}")
        logger.exception(e)
    finally:
        source_sftp.close()
        dest_sftp.close()

def main():
    args = build_args()
    PipeProcessor()\
        .input(traverse(args))\
        .pipe(worker_fn=sync_file, mode="process", args=(args,), num_workers=args.num_workers, inq_size=args.batch_size * args.num_workers, batch_size=args.batch_size, name="Sync Files")\
        .start().join()

if __name__ == "__main__":
    main()
