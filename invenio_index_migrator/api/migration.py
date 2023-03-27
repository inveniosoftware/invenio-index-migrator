# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing API."""

from __future__ import absolute_import, print_function

from elasticsearch.exceptions import NotFoundError
from invenio_search.proxies import current_search_client
from werkzeug.utils import cached_property

from ..proxies import current_index_migrator
from ..utils import ESClient, State, obj_or_import_string


def ensure_valid_config(f):
    """Decorate to ensure that all config parameters are valid."""
    def inner(self, name, **config):
        missing = [p for p in self.REQUIRED_PARAMS if p not in config]
        if missing:
            msg = "Required input parameters are missing {}" \
                .format(missing)
            raise Exception(msg)
        if config["strategy"] not in self.STRATEGIES:
            msg = "Invalid strategy {}. You should pass one of the {}." \
                .format(config["strategy"], self.STRATEGIES)
            raise Exception(description=msg)
        return f(self, name, **config)
    return inner


class Migration(object):
    """Index migration base class."""

    IN_CLUSTER_STRATEGY = "in_cluster_strategy"
    CROSS_CLUSTER_STRATEGY = "cross_cluster_strategy"

    STRATEGIES = (
        IN_CLUSTER_STRATEGY,
        CROSS_CLUSTER_STRATEGY
    )
    REQUIRED_PARAMS = ('src_es_client', 'jobs', 'strategy')

    @ensure_valid_config
    def __init__(self, name, **config):
        """Initialize the job configuration."""
        self.name = name
        self.jobs = {}
        self.index = current_index_migrator.config_index
        self.config = config
        self.src_es_client = ESClient(config['src_es_client'])
        self.state = State(
            index=self.index,
            document_id=name
        )

    @cached_property
    def strategy(self):
        """Return migration strategy."""
        return self.config["strategy"]

    @classmethod
    def create_from_config(cls, recipe_name, **recipe_config):
        """Create `Migration` instance from config."""
        return cls(recipe_name, **recipe_config)

    @classmethod
    def create_from_state(cls, recipe_name, **recipe_config):
        """Create `Migration` instance from ES state."""
        document = current_search_client.get(
            index=current_index_migrator.config_index, id=recipe_name)
        return cls(recipe_name, **document["_source"]["config"])

    def load_jobs_from_config(self):
        """Load jobs from config."""
        jobs = {}
        for job_name, job_config in self.config['jobs'].items():
            job = obj_or_import_string(job_config['cls'])(
                job_name, self, config=job_config)
            jobs[job_name] = job
        return jobs

    def create_index(self):
        """Create Elasticsearch index for the migration."""
        current_search_client.indices.create(index=self.index)
        print('[*] created index: {}'.format(self.index))

    def init(self, dry_run=False):
        """Initialize the index with recipe and jobs documents."""
        if not dry_run:
            if not current_search_client.indices.exists(index=self.index):
                self.create_index()
            try:
                current_search_client.get(index=self.index, id=self.name)
                raise Exception(
                    ('The document {} already exists, a job is already '
                     'active.').format(self.state.index))
            except NotFoundError:
                pass

        # Get old indices
        jobs = {}
        for job_name, job_config in self.config['jobs'].items():
            job = obj_or_import_string(job_config['cls'])(
                job_name, self, config=job_config)
            initial_state = job.initial_state(dry_run=dry_run)
            jobs[job_name] = (job, initial_state)
        self.jobs = jobs

        if not dry_run:
            migration_initial_state = {
                "type": "migration",
                "config": self.config,
                "status": "INITIAL",
                "job_ids": [job.document_name for job, _ in self.jobs.values()]
            }
            self.state.commit(migration_initial_state)

            for job, initial_state in self.jobs.values():
                job.state.commit(initial_state)
                job.create_index(initial_state["dst"]["index"])

    def rollover(self, force=False):
        """Perform a rollover action."""
        payload = dict(actions=[])
        self.jobs = self.load_jobs_from_config()

        if force or self.state.read()['status'] == 'COMPLETED':
            for job in self.jobs.values():
                payload['actions'] += job.rollover_actions()
            if payload["actions"]:
                current_search_client.indices.update_aliases(body=payload)
        else:
            print('Not all jobs are completed - rollover not possible.')

    def notify(self):
        """Notify when rollover is possible.

        Override this to notify the user whenever the threshold is reached and
        a rollover is possible.
        """
        pass

    def run(self):
        """Run the index sync job."""
        job_states = {}
        self.jobs = self.load_jobs_from_config()
        for name, job in self.jobs.items():
            print('[~] running job: {}'.format(name))
            job_states[name] = job.run()
        if all(state['threshold_reached'] for state in job_states.values()):
            state = self.state.read()
            state['status'] = 'COMPLETED'
            self.state.commit(state)
            self.notify()

    def status(self):
        """Get status for index sync job."""
        self.jobs = self.load_jobs_from_config()
        state = self.state.read()
        resp = dict(
            migration_status=state['status'],
            jobs={
                job_name: job.status() for job_name, job in self.jobs.items()
            }
        )
        if state['status'] == 'COMPLETED':
            print(
                'Threshold reach - rollover is possible. Please see the '
                'documentation for the steps needed to perform the rollover.'
            )
        return resp

    def cancel(self):
        """Cancel migration and all its jobs."""
        self.jobs = self.load_jobs_from_config()
        for job in self.jobs.values():
            job.cancel()

        state = self.state.read()
        state['status'] = 'CANCELLED'
        self.state.commit(state)
