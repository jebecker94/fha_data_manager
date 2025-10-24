"""Dictionary helpers for FHA snapshot schemas."""

from __future__ import annotations

from typing import Dict, List, Sequence

import numpy as np
import pyarrow as pa


SchemaMapping = Dict[str, str | type[np.generic]]


class FHADictionary:
    """Container for dictionary information covering FHA file types."""

    def __init__(self) -> None:
        self.single_family = self._SingleFamilyDictionary()
        self.hecm = self._HECMDictionary()

    class _SingleFamilyDictionary:
        def __init__(self) -> None:
            data_types: SchemaMapping = {
                'Property State': 'str',
                'Property City': 'str',
                'Property County': 'str',
                'Property Zip': 'Int32',
                'Originating Mortgagee': 'str',
                'Originating Mortgagee Number': 'Int32',
                'Sponsor Name': 'str',
                'Sponsor Number': 'Int32',
                'Down Payment Source': 'str',
                'Non Profit Number': 'Int64',
                'Product Type': 'str',
                'Loan Purpose': 'str',
                'Property Type': 'str',
                'Interest Rate': np.float64,
                'Mortgage Amount': 'Int64',
                'Year': 'Int16',
                'Month': 'Int16',
                'FHA_Index': 'str',
            }
            self.data_types: SchemaMapping = data_types

            schema_fields: Sequence[tuple[str, pa.DataType]] = [
                ('Property State', pa.string()),
                ('Property City', pa.string()),
                ('Property County', pa.string()),
                ('Property Zip', pa.int32()),
                ('Originating Mortgagee', pa.string()),
                ('Originating Mortgagee Number', pa.int32()),
                ('Sponsor Name', pa.string()),
                ('Sponsor Number', pa.int32()),
                ('Down Payment Source', pa.string()),
                ('Non Profit Number', pa.int64()),
                ('Product Type', pa.string()),
                ('Loan Purpose', pa.string()),
                ('Property Type', pa.string()),
                ('Interest Rate', pa.float64()),
                ('Mortgage Amount', pa.int64()),
                ('Year', pa.int16()),
                ('Month', pa.int16()),
                ('FHA_Index', pa.string()),
            ]
            self.schema: pa.Schema = pa.schema(schema_fields)
            self.column_names: List[str] = list(data_types.keys())

    class _HECMDictionary:
        def __init__(self) -> None:
            data_types: SchemaMapping = {
                'Property State': 'str',
                'Property City': 'str',
                'Property County': 'str',
                'Property Zip': 'Int32',
                'Originating Mortgagee': 'str',
                'Originating Mortgagee Number': 'Int32',
                'Sponsor Name': 'str',
                'Sponsor Number': 'Int32',
                'Sponsor Originator': 'str',
                'NMLS': 'Int64',
                'Standard/Saver': 'str',
                'Purchase/Refinance': 'str',
                'Rate Type': 'str',
                'Interest Rate': np.float64,
                'Initial Principal Limit': np.float64,
                'Maximum Claim Amount': np.float64,
                'Year': 'Int16',
                'Month': 'Int16',
                'HECM Type': 'str',
                'Current Servicer ID': 'Int64',
                'Previous Servicer ID': 'Int64',
            }
            self.data_types: SchemaMapping = data_types

            schema_fields: Sequence[tuple[str, pa.DataType]] = [
                ('Property State', pa.string()),
                ('Property City', pa.string()),
                ('Property County', pa.string()),
                ('Property Zip', pa.int32()),
                ('Originating Mortgagee', pa.string()),
                ('Originating Mortgagee Number', pa.int32()),
                ('Sponsor Name', pa.string()),
                ('Sponsor Number', pa.int32()),
                ('Sponsor Originator', pa.string()),
                ('NMLS', pa.int64()),
                ('Standard/Saver', pa.string()),
                ('Purchase/Refinance', pa.string()),
                ('Rate Type', pa.string()),
                ('Interest Rate', pa.float64()),
                ('Initial Principal Limit', pa.float64()),
                ('Maximum Claim Amount', pa.float64()),
                ('Year', pa.int16()),
                ('Month', pa.int16()),
                ('Current Servicer ID', pa.int64()),
                ('Previous Servicer ID', pa.int64()),
                ('FHA_Index', pa.string()),
            ]
            self.schema: pa.Schema = pa.schema(schema_fields)
            self.column_names: List[str] = list(data_types.keys())
