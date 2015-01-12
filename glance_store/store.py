# Copyright 2015 RedHat inc
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging

from oslo.config import cfg
from stevedore import driver
from stevedore import extension

from glance_store.common import utils
from glance_store import backend
from glance_store import exceptions
from glance_store import i18n
from glance_store import location

LOG = logging.getLogger(__name__)

_ = i18n._


class Store(object):
    """Initialize a a glance store instance.

    :param conf: glance store configuration instance
    :type conf: oslo_config.cfg.CONF
    """

    CONF = cfg.CONF

    DEPRECATED_STORE_OPTS = [
        cfg.DeprecatedOpt('known_stores', group='DEFAULT'),
        cfg.DeprecatedOpt('default_store', group='DEFAULT')
    ]

    _STORE_OPTS = [
        cfg.ListOpt('stores', default=['file', 'http'],
                    help=_('List of stores enabled'),
                    deprecated_opts=[_DEPRECATED_STORE_OPTS[0]]),
        cfg.StrOpt('default_store', default='file',
                   help=_("Default scheme to use to store image data. The "
                          "scheme must be registered by one of the stores "
                          "defined by the 'stores' config option."),
                   deprecated_opts=[_DEPRECATED_STORE_OPTS[1]])
    ]

    _STORE_CFG_GROUP = 'glance_store'


    def __init__(self, conf=CONF):
        self._conf = conf

    def _list_opts():
        driver_opts = []
        mgr = extension.ExtensionManager('glance_store.drivers')
        # NOTE(zhiyan): Handle available drivers entry_points provided
        drivers = [ext.name for ext in mgr]
        handled_drivers = []  # Used to handle backwards-compatible entries
        for store_entry in drivers:
            driver_cls = _load_store(None, store_entry, False)
            if driver_cls and driver_cls not in handled_drivers:
                if getattr(driver_cls, 'OPTIONS', None) is not None:
                    # NOTE(flaper87): To be removed in k-2. This should
                    # give deployers enough time to migrate their systems
                    # and move configs under the new section.
                    for opt in driver_cls.OPTIONS:
                        opt.deprecated_opts = [cfg.DeprecatedOpt(opt.name,
                                                                 group='DEFAULT')]
                        driver_opts.append(opt)
                handled_drivers.append(driver_cls)

        # NOTE(zhiyan): This separated approach could list
        # store options before all driver ones, which easier
        # to read and configure by operator.
        return ([(_STORE_CFG_GROUP, _STORE_OPTS)] +
                [(_STORE_CFG_GROUP, driver_opts)])

    def _register_opts(conf):
        opts = _list_opts()
        for group, opt_list in opts:
            LOG.debug("Registering options for group %s" % group)
            for opt in opt_list:
                conf.register_opt(opt, group=group)

    def _load_store(conf, store_entry, invoke_load=True):
        try:
            LOG.debug("Attempting to import store %s", store_entry)
            mgr = driver.DriverManager('glance_store.drivers',
                                       store_entry,
                                       invoke_args=[conf],
                                       invoke_on_load=invoke_load)
            return mgr.driver
        except RuntimeError:
            LOG.warn("Failed to load driver %(driver)s."
                     "The driver will be disabled" % dict(driver=driver))

    def _load_stores(conf):
        for store_entry in set(conf.glance_store.stores):
            try:
                # FIXME(flaper87): Don't hide BadStoreConfiguration
                # exceptions. These exceptions should be propagated
                # to the user of the library.
                store_instance = _load_store(conf, store_entry)

                if not store_instance:
                    continue

                yield (store_entry, store_instance)

            except exceptions.BadStoreConfiguration:
                continue

    def create_stores(conf=CONF):
        """
        Registers all store modules and all schemes
        from the given config. Duplicates are not re-registered.
        """
        store_count = 0

        for (store_entry, store_instance) in _load_stores(conf):
            try:
                schemes = store_instance.get_schemes()
                store_instance.configure()
            except NotImplementedError:
                continue
            if not schemes:
                raise exceptions.BackendException('Unable to register store %s. '
                                                  'No schemes associated with it.'
                                                  % store_entry)
            else:
                LOG.debug("Registering store %s with schemes %s",
                          store_entry, schemes)

                scheme_map = {}
                for scheme in schemes:
                    loc_cls = store_instance.get_store_location_class()
                    scheme_map[scheme] = {
                        'store': store_instance,
                        'location_class': loc_cls,
                    }
                location.register_scheme_map(scheme_map)
                store_count += 1

        return store_count

    def verify_default_store():
        scheme = CONF.glance_store.default_store
        try:
            get_store_from_scheme(scheme)
        except exceptions.UnknownScheme:
            msg = _("Store for scheme %s not found") % scheme
            raise RuntimeError(msg)

        def get_known_schemes():
            """Returns list of known schemes.
            """
            return location.SCHEME_TO_CLS_MAP.keys()


    def get(self, location):
        #get data from back store
        #return BackendObject

    def save(self, data, location=None):
        #save data

    def delete(self, location):
        #delete data
