
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
from .models.common_models import ScanTypeValue
from .models.data_scan import DataScan
from .models.table_scan import DDTableScan
from .models.dataset_scan import DDDatasetScan
from .models.output_models import (
    KEDatasetTable,
    KEDatasetRelationship,
    KEDatasetDetails,
    Query
)
from . import constants

def get_all_scans(project_id: str, location: str):
    url = KEDatasetScanHelper.DATAPLEX_LIST_SCANS_URL.format(
        project_id=project_id, 
        location=location
    )
    ke_auth = KEAuth()

    return ke_auth.get_url_content(url)

def get_scan(project_id: str, location: str, scan_id: str, full_view: bool = True):
    base_url = KEDatasetScanHelper.DATAPLEX_LIST_SCANS_URL.format(
        project_id=project_id, 
        location=location
    )
    suffix = '?view=FULL' if full_view else ''
    url = f"{base_url}/{scan_id}{suffix}"
    ke_auth = KEAuth()

    return ke_auth.get_url_content(url)

"""
  ------------------------------------------
  KEDatasetScanHelper
  ------------------------------------------
"""
# class NoKEScanFoundException(Exception): pass # deprecated
class NoDDScanFoundException(Exception): pass

class KEDatasetScanHelper(KEAuth):
    """A helper for interacting with the Knowledge Engine API."""
    DATAPLEX_BASE_URL = "https://dataplex.googleapis.com/v1"
    DATAPLEX_LIST_SCANS_URL = DATAPLEX_BASE_URL + "/projects/{project_id}/locations/{location}/dataScans"

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
        scan_url = self.DATAPLEX_LIST_SCANS_URL.format(
            base_url=self.DATAPLEX_BASE_URL,
            project_id=self.project_id,
            location=self.dataset_location
        )

        try:
            # print("\nSCAN URL: "+ scan_url + " \n")

            response = self.get_url_content(scan_url)

            #####
            # print(response)
            #####

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
            ## note: and scan.get('type') eliminates items without type

            if (  
                  scan.get('data') 
                  and scan.get('data').get('resource')
                  and scan.get('type') == ScanTypeValue.DATA_DOCUMENTATION.value
            ):
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
                full_scan_url = f"{self.DATAPLEX_BASE_URL}/{scan.name}?view=FULL"

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

                # if scan.type == ScanTypeValue.KNOWLEDGE_ENGINE.value: ## !! Deprecated
                #     new_scan = KEScan(**full_view_scan)

                if scan.type == ScanTypeValue.DATA_DOCUMENTATION:

                    if scan.is_for_table:
                        new_scan = DDTableScan(**full_view_scan)

                    if scan.is_for_dataset:
                        new_scan = DDDatasetScan(**full_view_scan)

                if new_scan:
                  self.__data_scans.append(new_scan)

        return self.__data_scans

    # deprecated
    # property # dataset knowledge engine scan, loop locally
    # def dataset_ke_scan(self) -> KEScan:
    #     for scan in self.dataplex_scans:
    #         if isinstance(scan, KEScan):
    #             return scan
    #     raise NoKEScanFoundException(f"No Knowledge Engine scan found for dataset {self.dataset_name}")

    @property # dataset data documentation scan, loop locally
    def dataset_dd_scan(self) -> DDDatasetScan:
        for scan in self.dataplex_scans:
            if isinstance(scan, DDDatasetScan):
                return scan
        raise NoDDScanFoundException(f"No Data Documentation scan found for dataset {self.dataset_name}")

    @property
    def dataset_description(self) -> str:
        return self.dataset_dd_scan.dataset_description

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


                    # print(type(scan.fields))
                    # print(type(scan.fields[0]))
                    # print(SchemaField(**scan.fields[0].model_dump()))
                    # print(type(scan.queries))
                    # print(type(scan.queries[0]))

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

    # deprecated
    # property
    # def dataset_business_glossary(self) -> List[BusinessTerm]:
    #     return self.dataset_ke_scan.business_glossary.terms

    @property
    def dataset_relationships(self) -> List[KEDatasetRelationship]:
        """
          This will require update when the relation representation becomes more complex.
          Currently should handle multple anded = conditions between left and right side.
        """
        project_dataset = self.project_id + '.' + self.dataset_name

        return_relationships = []

        relationships = self.dataset_dd_scan.schema_relationships
        for relationship in relationships:

          l_schema_paths = relationship.left_schema_paths
          l_table_fqn = l_schema_paths.table_fqn
          l_table_paths = l_schema_paths.paths
          l_table_sql_name = f"{project_dataset}.{l_table_fqn.split('/')[-1]}"
          if not self._table_is_allowed(l_table_fqn):
              continue

          r_schema_paths = relationship.right_schema_paths
          r_table_fqn = r_schema_paths.table_fqn
          r_table_paths = r_schema_paths.paths
          r_table_sql_name = f"{project_dataset}.{r_table_fqn.split('/')[-1]}"
          if not self._table_is_allowed(r_table_fqn):
              continue

          join_conditions = []

          for i, l_table_path in enumerate(l_table_paths):
              r_table_path = r_table_paths[i]
              new_join_condition = l_table_sql_name + '.' + l_table_path
              new_join_condition += ' = '
              new_join_condition += r_table_sql_name + '.' + r_table_path
              join_conditions.append(new_join_condition)

          return_relationships.append(KEDatasetRelationship(**{
              'table1': l_table_sql_name,
              'table2': r_table_sql_name,
              'relationship': ' AND '.join(join_conditions),
              'sources': relationship.sources,
              'confidence_score': relationship.confidence_score,
              'type': relationship.type,
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
            # "dataset_business_glossary": self.dataset_business_glossary, # deprecated
            "dataset_tables": self.dataset_tables
        })
