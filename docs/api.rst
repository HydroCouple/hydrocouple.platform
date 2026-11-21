Python API
==========

Start with :py:class:`hydrocouple_platform.client.PlatformClient` to submit work
and retrieve results. The :py:mod:`hydrocouple_platform.contracts` package defines
the messages shared by clients, agents and the control plane. For the wire
formats, see :doc:`contracts`.

For server integration, :py:func:`hydrocouple_platform.api.app.create_app`
constructs the HTTP application. The :py:mod:`hydrocouple_platform.services`
package implements its operations. The :py:mod:`hydrocouple_platform.agent`
and :py:mod:`hydrocouple_platform.runner` packages lease and execute work. On a running control plane, open ``/docs``
to browse HTTP operations and try requests, or fetch ``/openapi.json`` for
the machine-readable interface.

.. toctree::
   :maxdepth: 2

   hydrocouple_platform modules <autoapi/hydrocouple_platform/index>
