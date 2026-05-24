# SPDX-FileCopyrightText: 2015-2019 CERN.
# SPDX-License-Identifier: MIT

"""Index syncing tasks."""

from __future__ import absolute_import, print_function

from celery import shared_task

from .proxies import current_index_migrator


@shared_task(ignore_result=True)
def run_sync_job(job_id):
    """Run an index sync job by its ID."""
    job = current_index_migrator.jobs[job_id]
    job.run()
