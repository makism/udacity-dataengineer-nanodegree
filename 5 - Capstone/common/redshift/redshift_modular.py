""" Redshift SQL Queries

We have organized the different SQL queries in multiple clases, and
unified them under one class for readability and maintenance purposes.

"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

from .redshift_create import RedshiftCreate
from .redshift_drop import RedshiftDrop
from .redshift_errors import RedshiftErrors
from .redshift_insert import RedshiftInsert
from .redshift_qc import RedshiftQualityChecks
from .redshift_stage import RedshiftStage
from .redshift_udfs import RedshiftUDFs

    
class Redshift(RedshiftCreate, RedshiftStage, RedshiftInsert, RedshiftDrop, RedshiftQualityChecks, RedshiftErrors, RedshiftUDFs):
    """Unification of all statements."""
    ...
