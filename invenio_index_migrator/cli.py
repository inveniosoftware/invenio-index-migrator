# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing CLI commands."""

import json

import click
from flask.cli import with_appcontext
from invenio_search.cli import index as index_cmd

from .proxies import current_index_migrator


@index_cmd.group()
def migration():
    """Manage index migrations."""
    pass


@migration.command('init')
@with_appcontext
@click.argument('recipe_id')
@click.option('--yes-i-know', is_flag=True)
def init_migration(recipe_id, yes_i_know):
    """Initialize index migration."""
    recipe_config = current_index_migrator.recipes[recipe_id]
    migration_recipe = recipe_config['cls'].create_from_config(
        recipe_id,
        **recipe_config['params']
    )
    migration_recipe.init(dry_run=True)
    click.secho(
        '******* Information collected for this migration *******', fg='green')
    for job, initial_state in migration_recipe.jobs.values():
        pid_type = job.config['pid_type']
        state = initial_state
        dst = state['dst']
        click.secho('****************************', fg='green')
        click.echo('For pid_type: {}'.format(pid_type))
        click.echo('Index: {}'.format(dst.get('index')))
        click.echo('Aliases: {}'.format(dst.get('aliases')))
        click.echo('Mapping file path: {}'.format(dst.get('mapping')))

    confirm = yes_i_know or click.confirm(
        'Are you sure you want to apply this migration recipe?',
        default=False, abort=True
    )
    if confirm:
        migration_recipe.init()
    else:
        click.echo('Aborting migration...')


@migration.command('run')
@with_appcontext
@click.argument('recipe_id')
def run_migration(recipe_id):
    """Run current index migration."""
    recipe_config = current_index_migrator.recipes[recipe_id]
    migration_recipe = recipe_config['cls'].create_from_state(recipe_id)
    migration_recipe.run()


@migration.command('rollover')
@with_appcontext
@click.argument('recipe_id')
@click.option('--force', '-f', default=False, is_flag=True)
def rollover_sync(recipe_id, force=False):
    """Perform the index syncing rollover action."""
    recipe_config = current_index_migrator.recipes[recipe_id]
    migration_recipe = recipe_config['cls'].create_from_state(recipe_id)
    migration_recipe.rollover(force=force)


@migration.command('status')
@with_appcontext
@click.argument('recipe_id')
def status_sync(recipe_id):
    """Get current index syncing status."""
    recipe_config = current_index_migrator.recipes[recipe_id]
    migration_recipe = recipe_config['cls'].create_from_state(recipe_id)
    print(json.dumps(migration_recipe.status(), sort_keys=True, indent=2))


@migration.command('cancel')
@with_appcontext
@click.argument('recipe_id')
def cancel_sync(recipe_id):
    """Cancel the current index syncing."""
    recipe_config = current_index_migrator.recipes[recipe_id]
    migration_recipe = recipe_config['cls'].create_from_state(recipe_id)
    migration_recipe.cancel()
