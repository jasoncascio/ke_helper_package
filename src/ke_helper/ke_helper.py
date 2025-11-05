
"""
  ------------------------------------------
  KEDatasetScanHelper
  ------------------------------------------
"""
import json
import re
from typing import List
from google.cloud import bigquery
from pydantic import ValidationError
from .authentication import KEAuth
from .models.input_models import (
    DataScan,
    ScanTypeValue,
    KEScan,
    DDTableScan,
    DDDatasetScan,
    Query,
    BusinessTerm
)
from .models.output_models import (
    KEDatasetTable,
    KEDatasetRelationship,
    KEDatasetDetails
)
from . import constants


class NoKEScanFoundException(Exception): pass
class NoDDScanFoundException(Exception): pass


class KEDatasetScanHelper(KEAuth):
    """A helper for interacting with the Knowledge Engine API."""

    def __init__(self, project_id: str, dataset_name: str):
        super().__init__()
        self.dataset_name = dataset_name
        self.project_id = project_id
        self.__dataset_location = None
        self.__tables = []
        self.__data_scans = []
        self.__allowlist_tables = set()
        self.__blocklist_tables = set()
        self.__with_ddls = False
        self.__ddls = {}
        self.__with_table_counts = False
        self.__table_counts = {}

    def _flush(self):
        self.__tables.clear()
        self.__data_scans.clear()
        self.__ddls.clear()

    def _table_is_allowed(self, table_resource_fqn: str) -> bool:
        """
        Check if a table is allowed based on the allowlist and blocklist.
        The table resource FQN is in the format:
        //bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_name}/tables/{table_name}
        """
        short_table_name = table_resource_fqn.split('/')[-1]

        return (
            self._is_in_allowlist(short_table_name) and not
            self._is_in_blocklist(short_table_name)
        )

    def _is_in_allowlist(self, short_table_name: str) -> bool:
        if not self.__allowlist_tables:
            return True

        return short_table_name in self.__allowlist_tables

    def _is_in_blocklist(self, short_table_name: str) -> bool:
        if not self.__blocklist_tables:
            return False

        return short_table_name in self.__blocklist_tables

    def _get_dataset_table_names(self) -> List[str]:
        """ list of tables in shortname format """
        return_list = []
        client = bigquery.Client()
        dataset_ref = f"{self.project_id}.{self.dataset_name}"
        for table in client.list_tables(dataset_ref):
            return_list.append(table.full_table_id.split(".")[-1])

        return return_list

    def _get_scans_of_interest(self) -> List[DataScan]:
        scan_url = constants.DATAPLEX_LIST_SCANS_URL.format(
            project_id=self.project_id,
            location=self.dataset_location
        )

        try:
            response = self.get_url_content(scan_url)
        except Exception as e:
            print(f"Error fetching data scans: {e}")
            raise e

        try:
            scans = json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            raise e

        # Get the list of tables actually in the dataset at runtime (the KE API returns old stuff too)
        dataset_table_names = self._get_dataset_table_names()

        # Limit the scans to items in the requested dataset (per constructor)
        ds_test_string = f"/datasets/{self.dataset_name}"
        table_test_string = f"{ds_test_string}/tables/"

        scans_of_interest = []
        for scan in scans.get('dataScans', []):
            if scan.get('data') and scan.get('data').get('resource'):
                resource = scan.get('data').get('resource')

                if resource.endswith(ds_test_string) or table_test_string in resource:

                    try:
                      new_scan = DataScan(**scan)
                    except ValidationError as e:
                      print(f"Error creating DataScan object for {json.dumps(scan, indent=2)}:\n {e}")
                      raise e

                    if new_scan.is_for_table:
                        if self._table_is_allowed(new_scan.resource_name):
                            short_table_name = new_scan.resource_name.split('/')[-1]
                            if short_table_name in dataset_table_names:
                                scans_of_interest.append(new_scan)

                    if new_scan.is_for_dataset:
                        scans_of_interest.append(new_scan)

        return scans_of_interest

    @staticmethod
    def _get_bq_ddl_optimizations(
        ddl: str,
        optimization_type: str = 'PARTITION'
    ) -> List[str]:

        if optimization_type not in ['PARTITION', 'CLUSTER']:
            raise ValueError(f"Invalid optimization type: {optimization_type}")

        optimization_columns = []
        if optimization_type == 'PARTITION':
            regex = r"PARTITION BY\s+([a-zA-Z0-9_, ]+)"
        if optimization_type == 'CLUSTER':
            regex = r"CLUSTER BY\s+([a-zA-Z0-9_, ]+)"

        optimization_match = re.search(regex, ddl, re.IGNORECASE)
        if optimization_match:
            optimization_columns = [col.strip() for col in optimization_match.group(1).split(',')]

        return optimization_columns


    ## Options ##
    def with_table_list_constraints(self, allowlist: list = [], blocklist: list = []):
        """ configuration option """
        overlap = list(set(allowlist).intersection(set(blocklist)))
        if overlap:
            raise ValueError(f"Allowlist and blocklist cannot contain the same items: {overlap}")

        self._flush()
        self.__allowlist_tables.clear()
        self.__blocklist_tables.clear()
        self.__allowlist_tables.update(allowlist)
        self.__blocklist_tables.update(blocklist)

        return self

    def with_table_ddls(self, with_ddls=True):
        """ configuration option """
        self.__with_ddls = with_ddls
        self._flush()

        return self

    def with_table_counts(self, with_table_counts=True):
        """ configuration option """
        self.__with_table_counts = with_table_counts
        self._flush()

        return self

    ## Accessors ##
    @property
    def table_counts(self) -> dict:
        """ gets all the table counts for the dataset - row count, size_bytes"""
        if not self.__table_counts:
            client = bigquery.Client(project=self.project_id)
            query = f"""
                SELECT
                    CONCAT(project_id,'.',dataset_id,'.',table_id) AS fq_table_name
                , row_count
                , size_bytes
                FROM `{self.project_id}.{self.dataset_name}.__TABLES__`
            """
            query_job = client.query(query)
            results = query_job.result()

            for row in results:
                self.__table_counts[row.fq_table_name] = {
                    "row_count": row.row_count,
                    "size_bytes": row.size_bytes
                }

        return self.__table_counts

    @property
    def table_ddls(self) -> dict:
        """ gets all the table DDLs for the dataset """
        if not self.__ddls:
          client = bigquery.Client(project=self.project_id)
          query = f"""
              SELECT
                  CONCAT(
                      table_catalog,'.',table_schema,'.',table_name) AS fq_table_name,
                  ddl
              FROM `{self.project_id}.{self.dataset_name}.INFORMATION_SCHEMA.TABLES`
          """
          query_job = client.query(query)
          results = query_job.result()

          for row in results:
              self.__ddls[row.fq_table_name] = row.ddl

        return self.__ddls

    @property
    def dataset_location(self) -> str:
        if not self.__dataset_location:
            client = bigquery.Client()
            dataset = client.get_dataset(f'{self.project_id}.{self.dataset_name}')
            self.__dataset_location = dataset.location

        return self.__dataset_location

    @property
    def dataplex_scans(self) -> list:
        if not self.__data_scans:
            scans = self._get_scans_of_interest()

            for scan in scans:
                full_scan_url = f"{constants.DATAPLEX_BASE_URL}/{scan.name}?view=FULL"

                try:
                    response = self.get_url_content(full_scan_url)
                except Exception as e:
                    print(f"Error fetching data scans: {e}")
                    raise e

                try:
                    full_view_scan = json.loads(response)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON response: {e}")
                    raise e

                new_scan = None

                if scan.type == ScanTypeValue.KNOWLEDGE_ENGINE:
                    new_scan = KEScan(**full_view_scan)

                if scan.type == ScanTypeValue.DATA_DOCUMENTATION:
                    if scan.is_for_table:
                        print(f"Hydrating DDTableScan for {scan.name}")
                        print(scan.model_dump_json())
                        new_scan = DDTableScan(**full_view_scan)

                    if scan.is_for_dataset:
                        print(f"Hydrating DDDatasetScan for {scan.name}")
                        print(scan.model_dump_json())
                        new_scan = DDDatasetScan(**full_view_scan)

                if new_scan:
                  self.__data_scans.append(new_scan)

        return self.__data_scans

    @property # dataset knowledge engine scan, loop locally
    def dataset_ke_scan(self) -> KEScan:
        for scan in self.dataplex_scans:
            if isinstance(scan, KEScan):
                return scan
        raise NoKEScanFoundException(f"No Knowledge Engine scan found for dataset {self.dataset_name}")

    @property # dataset data documentation scan, loop locally
    def dataset_dd_scan(self) -> DDDatasetScan:
        for scan in self.dataplex_scans:
            if isinstance(scan, DDDatasetScan):
                return scan
        raise NoDDScanFoundException(f"No Data Documentation scan found for dataset {self.dataset_name}")

    @property
    def dataset_description(self) -> str:
        return self.dataset_ke_scan.dataset_description

    @property
    def dataset_tables(self) -> List[KEDatasetTable]:
        tables = []
        for scan in self.dataplex_scans:
            if isinstance(scan, DDTableScan):
                if self._table_is_allowed(scan.resource_name): # This is already filtered

                    ddl = None
                    partition_columns = None
                    cluster_columns = None
                    if self.__with_ddls:
                        ddl = self.table_ddls.get(scan.full_table_name, None)
                        if ddl:
                            partition_columns = self._get_bq_ddl_optimizations(
                                ddl=ddl
                            )
                            cluster_columns = self._get_bq_ddl_optimizations(
                                ddl=ddl, optimization_type='CLUSTER'
                            )

                    row_count = None
                    size_bytes = None
                    if self.__with_table_counts:
                        table_counts = self.table_counts.get(scan.full_table_name, None)
                        if table_counts:
                          row_count = table_counts.get("row_count")
                          size_bytes = table_counts.get("size_bytes")

                    tables.append(KEDatasetTable(**{
                        "name": scan.full_table_name,
                        "overview": scan.overview,
                        "fields": scan.fields,
                        "queries": scan.queries,
                        "ddl": ddl,
                        "row_count": row_count,
                        "size_bytes": size_bytes,
                        "partition_columns": partition_columns,
                        "cluster_columns": cluster_columns,
                    }))

        return tables

    @property
    def dataset_queries(self) -> List[Query]:
        return self.dataset_dd_scan.queries

    @property
    def dataset_business_glossary(self) -> List[BusinessTerm]:
        return self.dataset_ke_scan.business_glossary.terms

    @property
    def dataset_relationships(self) -> List[KEDatasetRelationship]:
        """
          This will require update when the relation representation becomes more complex.
          Currently should handle multple anded = conditions between left and right side.
        """
        project_dataset = self.project_id + '.' + self.dataset_name

        return_relationships = []

        relationships = self.dataset_ke_scan.schema_relationships
        for relationship in relationships:

          left_tuples = relationship.left_columns_tuple
          table1_fqn = left_tuples[0].entry_fqn
          table1_sql_name = f"{project_dataset}.{table1_fqn.split('/')[-1]}"
          if not self._table_is_allowed(table1_fqn):
              continue

          right_tuples = relationship.right_columns_tuple
          table2_fqn = right_tuples[0].entry_fqn
          table2_sql_name = f"{project_dataset}.{table2_fqn.split('/')[-1]}"
          if not self._table_is_allowed(table2_fqn):
              continue

          join_conditions = []

          for i, left_item in enumerate(left_tuples):
              right_item = right_tuples[i]
              new_join_condition = table1_sql_name + '.' + left_item.field_path
              new_join_condition += ' = '
              new_join_condition += table2_sql_name + '.' + right_item.field_path
              join_conditions.append(new_join_condition)

          return_relationships.append(KEDatasetRelationship(**{
              'table1': table1_sql_name,
              'table2': table2_sql_name,
              'relationship': ' AND '.join(join_conditions),
              'source': 'LLM-inferred'
          }))

        return return_relationships

    @property
    def dataset_all_details(self) -> KEDatasetDetails:
        return KEDatasetDetails(**{
            "project_id": self.project_id,
            "dataset_name": self.dataset_name,
            "dataset_location": self.dataset_location,
            "dataset_description": self.dataset_description,
            "dataset_relationships": self.dataset_relationships,
            "dataset_queries": self.dataset_queries,
            "dataset_business_glossary": self.dataset_business_glossary,
            "dataset_tables": self.dataset_tables
        })