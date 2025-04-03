
import logging
import multiprocessing
import multiprocessing.queues
import os
import queue
import threading
from typing import Iterable
import time
import random

from tqdm import tqdm

from sftp_transfer.logger import get_logger

logger = get_logger(__name__, level=logging.DEBUG if "DEBUG" in os.environ else logging.INFO)

class _Processor:
    def __init__(
        self,
        worker_fn: callable,
        mode: str,
        batch_size: int,
        num_workers: int,
        name="",
        args: tuple=(),
        kwargs: dict={},
        inq_size=0,
        outq_size=0,
    ):
        self.worker_fn = worker_fn
        self.mode = mode
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.args = args
        self.kwargs = kwargs

        self.name = name

        self.inq_size = inq_size
        self.outq_size = outq_size

        self.in_q: multiprocessing.Queue = None
        self.out_q: multiprocessing.Queue = None
        self.processes = []

        self.progress: multiprocessing.Queue = None
        self.progress_process = None
    
    # def get_inq(self):
    #     if self.in_q is None:
    #         self.in_q = multiprocessing.Queue(self.inq_size)
    #     return self.in_q
        

class PipeProcessor:
    def __init__(self):
        self.datas = []
        self.steps: list[_Processor] = []
        self.input_process = None

        self.manager = multiprocessing.Manager()


    def input(self, datas: Iterable):
        """
        设置输入数据和参数

        :param datas: 可迭代的数据对象，作为管道处理的输入数据
        :return: 返回当前的 PipeProcessor 实例，以便进行链式调用
        """
        self.datas = datas
        return self

    def pipe(
        self,
        worker_fn: callable,
        mode="thread",
        batch_size=1,
        num_workers=1,
        inq_size=0,
        name="",
        args: tuple=(),
        kwargs: dict={},
    ):
        """
        向管道中添加一个处理步骤。

        :param worker_fn: 用于处理数据的可调用对象，例如函数。
        :param mode: 处理模式，可以是 "process" 或 "thread"，默认为 "thread"。
        :param batch_size: 每个批次处理的数据数量，默认为 1。
        :param num_workers: 处理该步骤的工作进程或线程数量，默认为 1。
        :param inq_size: 输入队列的大小，默认为 0。
        :param name: 该处理步骤的名称，默认为 None。
        :param args: 传递给 worker_fn 的位置参数，默认为空元组。
        :param kwargs: 传递给 worker_fn 的关键字参数，默认为空字典。
        :return: 返回当前的 PipeProcessor 实例，以便进行链式调用。
        """
        self.steps.append(
            _Processor(
                worker_fn,
                mode,
                batch_size,
                num_workers,
                name=name,
                inq_size=inq_size,
                args=args,
                kwargs=kwargs,
            )
        )
        return self

    def start(self):
        """
        启动pipeline
        """
        # self.manager.start()
        self.input_process = self._start_input()
        for i, _step in enumerate(self.steps):
            self._start_step(i)
        return self

    def join(self):
        """
        等待pipeline处理完成
        """
        self._join_worker(self.input_process)
        logger.info("Input worker finished")
        # self._send_stop_signal(self.steps[0].in_q, self.steps[0].num_workers)

        for step in self.steps:
            self._send_stop_signal(step.in_q, step.num_workers)

            for process_index, process in enumerate(step.processes):
                pid = process.pid if isinstance(process, multiprocessing.Process) else process.native_id

                while process.is_alive():
                    logger.info(f"[PID {pid}] Processor {step.name}-worker-{process_index} joining.")
                    self._join_worker(process)

            if step.progress is not None:
                step.progress.put(None)
            if step.progress_process is not None:
                self._join_worker(step.progress_process, timeout=10)

        self.manager.shutdown()
        return self

    def _send_stop_signal(self, q: multiprocessing.Queue, num_workers: int):
        for _ in range(num_workers):
            q.put(None)

    def _join_worker(self, p, timeout=None):
        # if p.is_alive():
        p.join(timeout=timeout)
        # if isinstance(p, multiprocessing.Process) and p.is_alive():
            # p.close()

    def _input_worker(self, in_q: multiprocessing.Queue, datas, end_signal_count):
        try:
            for dataframe in datas:
                in_q.put(dataframe)
            logger.info("Input traversing done.")
        except Exception as ex:
            logger.exception(ex)
        # finally:
            # self._send_stop_signal(in_q, end_signal_count)

    def _start_input(self):
        """
        启动input worker,将数据输入到第一个step的input queue
        """
        in_q = self.manager.Queue(self.steps[0].inq_size)
        self.steps[0].in_q = in_q
        end_signal_count = self.steps[0].num_workers
        t = threading.Thread(target=self._input_worker, args=(in_q, self.datas, end_signal_count))
        t.start()

        return t

    def _has_next(self, i: int):
        return len(self.steps) - 1 > i

    def _start_progress_monitor(self, processor: _Processor):
        def _loop_progress(q: multiprocessing.Queue, name):
            with tqdm(desc=name) as pbar:
                while True:
                    progress = self._shift_queue(q)
                    if progress is None:
                        break
                    pbar.update(progress)
        processor.progress = self.manager.Queue()
        processor.progress_process = threading.Thread(target=_loop_progress, args=(processor.progress, processor.name))
        processor.progress_process.start()
        return processor.progress

    def _start_step(self, i: int):
        """
        启动第i个worker
        """
        step = self.steps[i]
        if step.in_q is None:
            step.in_q = self.manager.Queue(step.inq_size)

        if self._has_next(i):
            self.steps[i+1].in_q = self.manager.Queue(self.steps[i+1].inq_size)
            out_q = self.steps[i+1].in_q
            step.out_q = out_q

        self._start_progress_monitor(step)
        for worker_id in range(step.num_workers):
            process = self._start_processor(i, worker_id)
            step.processes.append(process)

        return step

    def _start_processor(self, i: int, worker_id: int):
        processor = self.steps[i]
        if self._has_next(i):
            end_signal_count = self.steps[i+1].num_workers
        else:
            end_signal_count = 0
        kwargs = {
            "fn": processor.worker_fn,
            "batch_size": processor.batch_size,
            "in_q": processor.in_q,
            "out_q": processor.out_q,
            "args": processor.args,
            "kwargs": processor.kwargs,
            "progress": processor.progress,
            "worker_id": worker_id,
            "num_workers": processor.num_workers,
            "end_signal_count": end_signal_count,
        }

        if processor.mode == "process":
            process = multiprocessing.Process(
                target=self._loop_process,
                kwargs=kwargs,
            )
        elif processor.mode == "thread":
            process = threading.Thread(
                target=self._loop_process,
                kwargs=kwargs,
            )
        else:
            raise ValueError(f"Unknown mode {processor.mode}")

        process.start()
        name = processor.name if processor.num_workers == 1 else f"{processor.name}-worker-{len(processor.processes)}"
        logger.info(f"Processor {name} started")
        return process


    def _loop_process(self, fn: callable, batch_size: int, in_q: multiprocessing.Queue, out_q: multiprocessing.Queue, args, kwargs, progress, worker_id: int, num_workers: int, end_signal_count: int):
        batch = []
        try:
            while True:
                item = self._shift_queue(in_q)
                if item is None:
                    break
                batch.append(item)
                if len(batch) >= batch_size:
                    input_data = None
                    if len(batch) == 1:
                        input_data = batch[0]
                    elif len(batch) > 1:
                        input_data = batch
                    try:
                        result = fn(input_data, *args, **kwargs)
                        if out_q is not None:
                            assert result is not None
                            out_q.put(result)
                    except Exception as ex:
                        logger.error(f"Error in worker function {fn.__name__}: {ex}")
                        logger.exception(ex)
                    if progress is not None:
                        progress.put(len(batch))
                    batch = []

            if len(batch) > 0:
                input_data = None
                if len(batch) == 1:
                    input_data = batch[0]
                elif len(batch) > 1:
                    input_data = batch
                try:
                    result = fn(input_data, *args, **kwargs)
                    if out_q is not None:
                        assert result is not None
                        out_q.put(result)
                except Exception as ex:
                    logger.error(f"Error in worker function {fn.__name__}: {ex}")
                    logger.exception(ex)
                if progress is not None:
                    progress.put(len(batch))
        
        except Exception as ex:
            logger.error(f"Error in worker {fn.__name__}: {ex}")
            logger.exception(ex)


    def _shift_queue(self, q: multiprocessing.Queue, max_tries=60 * 10):
        times = 0
        while True:
            times += 1
            if times >= max_tries:
                logger.warning(f"Cannot shift queue. Max tries {max_tries} reached")
                return None
            try:
                result = q.get(block=False)
                return result
            except queue.Empty:
                logger.debug(f"Get Queue Failed (qsize {q.qsize()}). Retry {times} times.")
                time.sleep(random.random() * 2)
                continue
            except Exception as ex:
                continue
