"""This file contains functionality for doing parallelized notebook jobs."""

# Typing imports
from __future__ import annotations
from typing import Any, List, Optional

# Python imports
from concurrent.futures import ThreadPoolExecutor


class NotebookData:
    """A class encapsulating a notebook.

    This class is used to call notebooks from another notebook. It contains
    parameters to manage the behavior of the notebooks.
    """

    def __init__(self,
                 path: str,
                 timeout: int,
                 dbutils: Any,
                 parameters: Optional[dict] = None,
                 retry: int = 0):
        """Log setup parameters.

        :param str path: The path to the notebook to wrap.
        :param int timeout: Kill the running notebook after this duration (seconds)
        :param dbutils: A ``dbutils`` object
        :param dict parameters: A dict of parameters to pass to the called notebook
        :param int retry: The number of times to retry
        """
        self.path = path
        self.timeout = timeout
        self.dbutils = dbutils
        self.parameters = parameters
        self.retry = retry

    def submit_notebook(self):
        """Begins notebook execution.

        This method kicks off execution for its associated notebook.
        """
        print(f"Running notebook {self.path}")

        try:
            if self.parameters is not None:
                return self.dbutils.notebook.run(
                    self.path,
                    self.timeout,
                    self.parameters
                )
            return self.dbutils.notebook.run(self.path, self.timeout)

        except Exception:
            if self.retry < 1:
                raise

            print(f"Retrying notebook {self.path}")
            self.retry = self.retry - 1
            return self.submit_notebook()


def parallel_notebooks(notebooks: List[NotebookData],
                       num_in_parallel: int
                       ) -> list:
    """Execute multiple notebooks in parallel.

    The purpose of this function is to take a list of ``NotebookData`` objects
    and execute ``num_in_parallel`` of them at a time, returning the result of
    all executions at the end.

    Note: If you create too many notebooks in parallel the driver may crash when
    you submit all of the jobs at once. This code limits the number of parallel
    notebooks.

    :param [NotebookData] notebooks: A list of notebooks to execute
    :param int num_in_parallel: The maximum number of notebooks to run
                                in parallel
    :return: A list of the returned objects from each notebook
    :rtype: ``list``
    """
    with ThreadPoolExecutor(max_workers=num_in_parallel) as ec:
        return [ec.submit(NotebookData.submit_notebook, notebook)
                for notebook in notebooks]


def validate_notebook_results(results: list, run_labels: List[str], stage_label: str):
    """Make sure no exceptions are raised.

    This function parses the results from a list of notebooks and ensures
    no errors are present.

    :param list results: A list of results from notebook executions
    :param [str] run_labels: A list of labels for each run
    :param str stage_label: A label to give the final error message
    :raises ValueError: If any of the runs errored out
    """
    num_exceptions = 0

    for key, res in zip(run_labels, results):
        if res is not None and res._exception is not None:
            num_exceptions += 1
            print(f'{key}: {res._exception}')

    if num_exceptions > 0:
        raise ValueError(f"{stage_label}: Run failed for {num_exceptions} notebook{'s' if num_exceptions != 1 else ''}")