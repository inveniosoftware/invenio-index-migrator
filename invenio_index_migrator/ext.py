# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Index syncing extension."""

from __future__ import absolute_import, print_function

from flask import current_app
from werkzeug.utils import cached_property

from . import config
from .cli import index_cmd
from .utils import build_alias_name, obj_or_import_string


class InvenioIndexMigrator(object):
    """Invenio index sync extension."""

    def __init__(self, app=None, **kwargs):
        """Extension initialization.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        self._clients = {}

        if app:
            self.init_app(app, **kwargs)

    @cached_property
    def recipes(self):
        """Get all configured migration recipes."""
        recipes_config = current_app.config.get('INDEX_MIGRATOR_RECIPES', {})
        for recipe_id, recipe_cfg in recipes_config.items():
            recipe_cfg['cls'] = obj_or_import_string(recipe_cfg['cls'])
        return recipes_config

    @cached_property
    def config_index(self):
        """Return migration index."""
        return build_alias_name(
            current_app.config['INDEX_MIGRATOR_INDEX_NAME']
        )

    def init_app(self, app):
        """Flask application initialization.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        self.init_config(app)
        app.cli.add_command(index_cmd)
        app.extensions['invenio-index-migrator'] = self

    @staticmethod
    def init_config(app):
        """Initialize configuration.

        :param app: An instance of :class:`~flask.app.Flask`.
        """
        for k in dir(config):
            if k.startswith('INDEX_MIGRATOR_'):
                app.config.setdefault(k, getattr(config, k))
