# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing CLI commands."""

import click
from datetime import datetime

from flask import current_app
from flask.cli import with_appcontext
from invenio_search.cli import index as index_cmd
from .proxies import current_index_migrator
from .utils import obj_or_import_string


@index_cmd.group()
def sync():
    """Manage index syncing."""
    pass


@sync.command('init')
@with_appcontext
@click.argument('recipe_id')
@click.option('--yes-i-know', is_flag=True)
def init_sync(recipe_id, yes_i_know):
    """Initialize index syncing."""
    job_config = current_index_migrator.jobs[recipe_id]
    sync_job = job_config['cls'](**job_config['params'])

    recipe = sync_job.init(dry_run=True)
    click.secho(
        '******* Information collected for this migration *******', fg='green')
    for job in recipe:
        pid_type = job['pid_type']
        dst = job['dst']
        click.secho('****************************', fg='green')
        click.echo('For pid_type: {}'.format(pid_type))
        click.echo('Index: {}'.format(dst['index']))
        click.echo('Aliases: {}'.format(dst['aliases']))
        click.echo('Mapping file path: {}'.format(dst['mapping']))
        click.echo('Doc type: {}'.format(dst['doc_type']))

    confirm = yes_i_know or click.confirm(
        'Are you sure you want to apply this recipe?',
        default=False, abort=True
    )
    if confirm:
        sync_job.init()
    else:
        click.echo('Aborting migration...')


@sync.command('run')
@with_appcontext
@click.argument('recipe_id')
def run_sync(recipe_id):
    """Run current index syncing."""
    job = current_index_migrator.jobs[recipe_id]
    sync_job = job['cls'](**job['params'])
    sync_job.run()


@sync.command('rollover')
def rollover_sync():
    """Perform the index syncing rollover action."""
    pass


@sync.command('status')
@with_appcontext
@click.argument('recipe_id')
def status_sync(recipe_id):
    """Get current index syncing status."""
    recipe = current_index_migrator.jobs[recipe_id]
    sync_recipe = recipe['cls'](**recipe['params'])
    click.echo('================ Status ================')
    for job in sync_recipe.status():
        click.echo('Job #{job_index} - {pid_type}\n'.format(
            job_index=job['job_index'], pid_type=job['job']['pid_type']
        ))
        click.echo('Status: {}'.format(job['status']))
        click.echo('Last updated: {}'.format(
            datetime.fromtimestamp(float(job['last_updated']))
        ))
        click.echo('Jobs in queue: {}'.format(job['queue_size']))
        click.echo('Threshold reached: {}'.format(job['job']['threshold_reached']))
        if 'duration' in job:
            click.echo('Duration: {:.1f} seconds'.format(
                job['duration'] / 1000000000.0))
        if job['completed']:
            click.echo('Took: {} seconds'.format(job['seconds']))
            click.echo('Reindexed: {} records'.format(job['total']))
        else:
            if 'current' in job and 'total' in job:
                click.echo('Progress: {current}/{total} ({percent}%)'.format(
                    **job
                ))
        click.echo('----------------------------------------')


@sync.command('cancel')
def cancel_sync():
    """Cancel the current index syncing."""
    pass
