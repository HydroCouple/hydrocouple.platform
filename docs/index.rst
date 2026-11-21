HydroCouple execution platform
==============================

Submit a SWMM simulation or HydroCouple composition to a compute target you control. The control plane stores the request and its history in PostgreSQL. An agent at the target leases work, runs it on the local scheduler and reports the result. Use the run ID to retrieve the artifacts and identify the attempt that produced them.

.. toctree::
   :maxdepth: 2

   guide
   reference
   api
