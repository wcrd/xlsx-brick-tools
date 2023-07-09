from argparse import ArgumentError
import rdflib
from rdflib.plugins.stores.memory import Memory
from rdflib.plugins import sparql
import logging
import os.path
import pkgutil
import io
import pandas as pd
from datetime import datetime
import sys
from ..common.relationships import (
    BRICK_RELATIONSHIPS,
    SWITCH_RELATIONSHIPS
)
from ..common import helpers, sparql_queries as sq
from . import triple_generator as tg

from typing import TypedDict

class CustomOntology(TypedDict):
    ttl_path: str
    name: str

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Move common methods from Graph into here so we can do multi-inheritance
# TODO: Update Graph() and Dataset() to inherit from this. Need to make the methods more generic.
# class GeneratorMethods:
#     def process(self, path_to_xlsx: str, portfolio_name: str = "example", building_name: str = "example_building", relationship_field:tuple = ("Brick", "identifier")):
#         if not os.path.isfile(path_to_xlsx):
#             logger.error(f"File not found at specified path: {path_to_xlsx}")
#             sys.exit('Error: Input file not found')

#         logger.info("Loading file...")
#         # load sheets for: Locations, Equipment, Points
#         xlFile = pd.ExcelFile(path_to_xlsx)
#         df_locations = pd.read_excel(xlFile, sheet_name="locations", header=[0, 1], dtype=str)
#         df_equipment = pd.read_excel(xlFile, sheet_name="equipment", header=[0, 1], dtype=str)
#         df_points = pd.read_excel(xlFile, sheet_name="points", header=[0, 1], dtype=str)

#         df_locations.name = "locations"
#         df_equipment.name = "equipment"
#         df_points.name = "points"


#         # tidy dfs
#         logger.info("Clearing nulls.")
#         df_locations.fillna(0, inplace=True)
#         df_equipment.fillna(0, inplace=True)
#         df_points.fillna(0, inplace=True)
#         logger.info("Removing non-valid chars.")
#         df_locations.replace({u'\xa0': u' '}, regex=True, inplace=True)
#         df_equipment.replace({u'\xa0': u' '}, regex=True, inplace=True)
#         df_points.replace({u'\xa0': u' '}, regex=True, inplace=True)

#         logger.info("File load completed.")

#         # NAMESPACES
#         logger.info("Generating building namespace...")
#         BUILDING = rdflib.Namespace(f"https://{portfolio_name}.com/{building_name}#")
#         self._namespaces['building'] = BUILDING
#         self.bind('building', BUILDING)
#         self._namespaces['ref'] = BUILDING  # used for relative relationship references
#         META = rdflib.Namespace("https://meta.com#") # temporary namespace to hold the metadata items that are used for tags. TODO: Update this.
#         self._namespaces['meta'] = META
#         self.bind('meta', META)
#         self._building = {
#             'portfolio': portfolio_name,
#             'building': building_name
#         }
#         logger.info("Namespace generation complete.")

#         # PROCESS EXCEL DATA & GENERATE TRIPLES
#         logger.info("Processing Building Model data...")

#         # validate relationship column exists
#         logger.info(f"Relationships defined by referencing column: {relationship_field}. Validating column exists on all sheets...")
#         for df in [df_equipment, df_locations, df_points]:
#             if not helpers.column_exists(df, relationship_field):
#                 logger.error(f"Model input sheet: {df.name} does not have column: {relationship_field} defined. Aborting.")
#                 sys.exit("Error: valid reference column not found.")
#             else:
#                 logger.info(f"OK. {df.name} reference column found.")
        
#         # generate id<>relationship_field map (entites must be related via the identifier (subject) field in the rdf graph)
#         # if no custom relationship is provided this is not required.
#         logger.info(f"Generating {relationship_field}<>identifier entity lookup table... ")
#         df_map = pd.DataFrame(columns=['subject', 'custom'])
#         for df in [df_equipment, df_locations]:
#             df_temp = df[[("Brick", "identifier"), relationship_field]]
#             df_temp.columns = ['subject', 'custom']
#             df_map = df_map.append(df_temp) # this does not consider dups. They should be identified using the validation package.
#         self._df_map = df_map
#         logger.info("Successfully generated.")
#         # df_map.to_csv("./_debug.csv")
#         # return

#         logger.info("Processing Locations...")
#         triples_locations = tg.process_df(df_locations, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
#         triples_locations.extend(tg.process_df(df_locations, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

#         logger.info("Processing Equipment...")
#         triples_equipment = tg.process_df(df_equipment, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
#         triples_equipment.extend(tg.process_df(df_equipment, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

#         logger.info("Processing Points...")
#         triples_points = tg.process_df(df_points, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
#         triples_points.extend(tg.process_df(df_points, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))
#         logger.info("Building model data successfully processed.")

#         # ADD TRIPLES TO GRAPH
#         logger.info("Adding Entities to model...")
#         for triple in [*triples_locations, *triples_equipment, *triples_points]:
#             self.add(triple)
#         logger.info(f"{len(triples_locations)} location triples added.")
#         logger.info(f"{len(triples_equipment)} equipment triples added.")
#         logger.info(f"{len(triples_points)} point triples added.")

#         logger.info("Generating inverse relationships...")
#         self.update(sq.generate_inverse_relationships())

#         # Process Extensions
#         logger.info("Processing model extensions.")
#         # SwitchTags
#         logger.info("Processing SwitchTags")
#         logger.info("Processing Equipment tags")
#         tg.process_tags(self, df_equipment, self._namespaces)
#         logger.info("Processing Location tags")
#         tg.process_tags(self, df_locations, self._namespaces)
#         logger.info("Processing Point tags")
#         tg.process_tags(self, df_points, self._namespaces)

#         logger.info("Entities successfully added to model.")
#         logger.info("Processing complete.")


# New named graph approach
class Dataset(rdflib.Dataset):
    def __init__(
            self, 
            load_brick: bool = True, 
            load_switch: bool = True, 
            brick_version: str = "1.2", 
            switch_version: str = "1.1.5",
            path_to_local_brick: str = None, 
            path_to_local_switch: str = None, 
            custom_graph: CustomOntology = None,
            store=Memory(), 
            sparql_load_graphs=False
        ):
        """
        @params:
        load_brick: True; whether to load Brick into the final graph. Recommended as is required for populating inverses, etc.
                    Set this to False if you want to load a local version of Brick via the 'path_to_local_brick' parameter.    
        load_switch: True; whether to load Switch into the final graph. Recommended as is required for populating inverses, etc.
                    Set this to False if you want to load a local version of Switch via the 'path_to_local_switch' parameter.
        path_to_local_brick: Absolute path to local TTL to use as the Brick ontology. Only evaluated if load_brick=False
        path_to_local_switch: Absolute path to local TTL to use as the Switch ontology. Only evaluated if load_switch=False
        custom_graph: expects a dict of shape { graph: rdflib.Graph, name: string }. The name is used for namespacing the graph depending on store type. It is not used in this class.

        """
        self._ontology_versions = {
            'brick_version': brick_version,
            'switch_version': switch_version
        }
        self._building = {}
        self._namespaces = {}
        self._graph_namespace = rdflib.Namespace("https://_graph_.com#")

        # Create Dataset
        # We want the default graph to be a union of all (i.e. a ConjunctiveGraph())
        super().__init__(store=store, default_union=True)
        # set this for sparql to play nice
        sparql.SPARQL_LOAD_GRAPHS = sparql_load_graphs

        # Create sub-graphs
        if load_brick:
            # get ontology data from package
            data = pkgutil.get_data(
                __name__, f"../common/ontologies/Brick/{brick_version}/Brick.ttl"
            ).decode()
            # wrap in StringIO to make it file-like
            self.graph(self._graph_namespace['brick']).parse(source=io.StringIO(data), format="turtle")
        else:
            # check for local brick path
            if path_to_local_brick:
                self.graph(self._graph_namespace['brick']).parse(path_to_local_brick, format="turtle")

        if load_switch:
            # get ontology data from package
            data = pkgutil.get_data(
                __name__, f"../common/ontologies/Switch/{switch_version}/Brick-SwitchExtension.ttl"
            ).decode()
            # wrap in StringIO to make it file-like
            self.graph(self._graph_namespace['switch']).parse(source=io.StringIO(data), format="turtle")
        else:
            # check for local switch path
            if path_to_local_switch:
                self.graph(self._graph_namespace['switch']).parse(path_to_local_switch, format="turtle")

        # load custom graph if exists
        if custom_graph:
            # names: brick, switch are reserved
            _reserved_names = ['brick', 'switch']
            if custom_graph['name'] in _reserved_names:
                raise ArgumentError(None, f"Custom graph name is not allowed. The following names are restricted: {_reserved_names}")
            self.parse(custom_graph['ttl_path'], format="turtle")
        
        self.generate_namespaces()

    def generate_namespaces(self):
        # create namespace objects to make querying easier
        self._namespaces = {name: rdflib.Namespace(URI) for name, URI in self.namespaces()}
    
    
    def process(self, path_to_xlsx: str, portfolio_name: str = "example", building_name: str = "example_building", relationship_field:tuple = ("Brick", "identifier"), graph_name:str = "building", process_source=True):
        if not os.path.isfile(path_to_xlsx):
            logger.error(f"File not found at specified path: {path_to_xlsx}")
            sys.exit('Error: Input file not found')

        # LOAD AND PROCESS INPUT FILE
        # This should be prevalidated by brick-xlsx-validator package
        logger.info("Loading file...")
        df_equipment, df_locations, df_points = helpers.import_model_template_file(path_to_xlsx)
        logger.info("File load completed.")

        # CREATE NEW GRAPH
        g = self.add_graph(self._graph_namespace[graph_name])

        # NAMESPACES
        logger.info("Generating building namespace...")

        self._building = {
            'portfolio': portfolio_name,
            'building': building_name
        }
        
        BUILDING = rdflib.Namespace(f"https://{portfolio_name}.com/{building_name}#")
        META = rdflib.Namespace("https://meta.com#") # temporary namespace to hold the metadata items that are used for tags. TODO: Update this.

        self._namespaces['building'] = BUILDING
        self._namespaces['ref'] = BUILDING  # used for relative relationship references
        self._namespaces['meta'] = META
        
        g.bind('building', BUILDING)
        g.bind('meta', META)
        
        logger.info("Namespace generation complete.")

        # PROCESS EXCEL DATA & GENERATE TRIPLES
        logger.info("Processing Building Model data...")

        # validate relationship column exists
        logger.info(f"Relationships defined by referencing column: {relationship_field}. Validating column exists on all sheets...")
        for df in [df_equipment, df_locations, df_points]:
            if not helpers.column_exists(df, relationship_field):
                logger.error(f"Model input sheet: {df.name} does not have column: {relationship_field} defined. Aborting.")
                sys.exit("Error: valid reference column not found.")
            else:
                logger.info(f"OK. {df.name} reference column found.")
        
        # generate id<>relationship_field map (entites must be related via the identifier (subject) field in the rdf graph)
        # if no custom relationship is provided this is not required.
        logger.info(f"Generating {relationship_field}<>identifier entity lookup table... ")
        df_map = pd.DataFrame(columns=['subject', 'custom'])
        for df in [df_equipment, df_locations]:
            df_temp = df[[("Brick", "identifier"), relationship_field]]
            df_temp.columns = ['subject', 'custom']
            df_map = df_map.append(df_temp) # this does not consider dups. They should be identified using the validation package.
        self._df_map = df_map
        logger.info("Successfully generated.")
        # df_map.to_csv("./_debug.csv")
        # return

        logger.info("Processing Locations...")
        triples_locations = tg.process_df(df_locations, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_locations.extend(tg.process_df(df_locations, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

        logger.info("Processing Equipment...")
        triples_equipment = tg.process_df(df_equipment, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_equipment.extend(tg.process_df(df_equipment, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

        logger.info("Processing Points...")
        triples_points = tg.process_df(df_points, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_points.extend(tg.process_df(df_points, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))
        logger.info("Building model data successfully processed.")

        # ADD TRIPLES TO GRAPH
        logger.info("Adding Entities to model...")
        for triple in [*triples_locations, *triples_equipment, *triples_points]:
            g.add(triple)
        logger.info(f"{len(triples_locations)} location triples added.")
        logger.info(f"{len(triples_equipment)} equipment triples added.")
        logger.info(f"{len(triples_points)} point triples added.")

        logger.info("Generating inverse relationships...")
        # need to look at the whole graph to generate inverses as we need the ontology files
        # TODO: This isn't going to work. We need to do inverses just for building, and write them to the building.
        # self.update(sq.generate_inverse_relationships())
        self.update(sq.generate_inverse_relationships_for_graph(), initBindings={"g": g.identifier})

        # Process Extensions
        logger.info("Processing model extensions.")
        # SwitchTags
        logger.info("Processing SwitchTags")
        logger.info("Processing Equipment tags")
        tg.process_tags(g, df_equipment, self._namespaces)
        logger.info("Processing Location tags")
        tg.process_tags(g, df_locations, self._namespaces)
        logger.info("Processing Point tags")
        tg.process_tags(g, df_points, self._namespaces)
        # Source
        if process_source==True:
            logger.info("Processing Source information")
            tg.process_source(g, df_points, self._namespaces)

        logger.info("Entities successfully added to model.")
        logger.info("Processing complete.")

    def export(self, export_mode: str = "building", export_path: str = os.path.join(os.getcwd(), "output"), timestamp: bool = True, graph_name:str = "building"):
        """
        Serialises a graph model to a TTL file and saves to given path
        Can generate a full, building only, or building equipment only model.

        :param timestamp: bool. Flag to include timestamp in filename for uniqueness
        :param export_path: dir to save ttl file. Defaults to CWD/output.
        :param export_mode: options = ["full", "building", "equipment_locations_systems"]
            * Default is building only.
            * A full model should rarely be used as it combines all source ontologies into one file
        :return:
        """
        # check path is OK
        if not os.path.exists(export_path):
            os.mkdir(export_path)

        # GENERATE TIMESTAMP FOR FILENAMES
        if timestamp:
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        else:
            timestamp_str = "Export"

        if export_mode == "full":
            logger.info("Exporting full model...")
            filename = f"{timestamp_str}_M_{self._building['portfolio']}_{self._building['building']}.ttl"
            self.serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")

        elif export_mode == "building":
            logger.info("Exporting building entities only brick model...")
            filename = f"{timestamp_str}_B_{self._building['portfolio']}_{self._building['building']}.ttl"
            self.graph(self._graph_namespace[graph_name]).serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")

        elif export_mode == "equipment_locations_systems":
            logger.info("Exporting equipment, location, system entities brick model...")

            logger.info("Generating new graph...(this can take a few minutes)")
            g = rdflib.Graph()
            g.bind("brick", self._namespaces['brick'])
            g.bind("building", self._namespaces['building'])
            g.bind("switch", self._namespaces['switch'])
            for item in self.query(sq.query_equipment_and_location_triples_in_namespace(self._namespaces), initBindings={'namespace': self._namespaces['building']}):
                g.add(item)

            logger.info("Exporting graph...")
            filename = f"{timestamp_str}_B_{self._building['portfolio']}_{self._building['building']}_noPoints.ttl"
            g.serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")
        else:
            logger.info(f"Provided export mode: {export_mode} is not supported.")

# Original all-in-one single graph method.
class Graph(rdflib.Graph):
    def __init__(
        self, 
        load_brick: bool = True, 
        load_switch: bool = True, 
        brick_version: str = "1.2", 
        switch_version: str = "1.1.7", 
        path_to_local_brick: str = None, 
        path_to_local_switch: str = None, 
        custom_graph: CustomOntology = None
        ):
        """
        @params:
        load_brick: True; whether to load Brick into the final graph. Recommended as is required for populating inverses, etc.
                    Set this to False if you want to load a local version of Brick via the 'path_to_local_brick' parameter.    
        load_switch: True; whether to load Switch into the final graph. Recommended as is required for populating inverses, etc.
                    Set this to False if you want to load a local version of Switch via the 'path_to_local_switch' parameter.
        path_to_local_brick: Absolute path to local TTL to use as the Brick ontology. Only evaluated if load_brick=False
        path_to_local_switch: Absolute path to local TTL to use as the Switch ontology. Only evaluated if load_switch=False
        custom_graph: expects a dict of shape { graph: rdflib.Graph, name: string }. The name is used for namespacing the graph depending on store type. It is not used in this class.

        """
        super().__init__()
        self._ontology_versions = {
            'brick_version': brick_version,
            'switch_version': switch_version
        }
        self._building = {}
        self._namespaces = {}

        if load_brick:
            # get ontology data from package
            data = pkgutil.get_data(
                __name__, f"../common/ontologies/Brick/{brick_version}/Brick.ttl"
            ).decode()
            # wrap in StringIO to make it file-like
            self.parse(source=io.StringIO(data), format="turtle")
        else:
            # check for local brick path
            if path_to_local_brick:
                self.parse(path_to_local_brick, format="turtle")

        if load_switch:
            # get ontology data from package
            data = pkgutil.get_data(
                __name__, f"../common/ontologies/Switch/{switch_version}/Brick-SwitchExtension.ttl"
            ).decode()
            # wrap in StringIO to make it file-like
            self.parse(source=io.StringIO(data), format="turtle")
        else:
            # check for local switch path
            if path_to_local_switch:
                self.parse(path_to_local_switch, format="turtle")
        
        # load custom graph if exists
        if custom_graph:
            self.parse(custom_graph['ttl_path'], format="turtle")

        self.generate_namespaces()

    def generate_namespaces(self):
        # generate callable Namespace objects from Graph
        namespaceURIs = dict(self.namespaces())
        # create namespace objects to make querying easier
        self._namespaces = {name: rdflib.Namespace(URI) for name, URI in namespaceURIs.items()}
        

    def load_ontology(self, ontology_name: str, ontology_version: str, path_to_ontology: str):
        # get ontology data from path
        if os.path.isfile(path_to_ontology):
            self.parse(path_to_ontology, format=rdflib.util.guess_format(path_to_ontology))
            self._ontology_versions[ontology_name] = ontology_version
        else:
            logger.error(f"File not found at specified path: {path_to_ontology}")

    def process(self, path_to_xlsx: str, portfolio_name: str = "example", building_name: str = "example_building", relationship_field:tuple = ("Brick", "identifier")):
        if not os.path.isfile(path_to_xlsx):
            logger.error(f"File not found at specified path: {path_to_xlsx}")
            sys.exit('Error: Input file not found')

        logger.info("Loading file...")
        # load sheets for: Locations, Equipment, Points
        xlFile = pd.ExcelFile(path_to_xlsx)
        df_locations = pd.read_excel(xlFile, sheet_name="locations", header=[0, 1], dtype=str)
        df_equipment = pd.read_excel(xlFile, sheet_name="equipment", header=[0, 1], dtype=str)
        df_points = pd.read_excel(xlFile, sheet_name="points", header=[0, 1], dtype=str)

        df_locations.name = "locations"
        df_equipment.name = "equipment"
        df_points.name = "points"


        # tidy dfs
        logger.info("Clearing nulls.")
        df_locations.fillna(0, inplace=True)
        df_equipment.fillna(0, inplace=True)
        df_points.fillna(0, inplace=True)
        logger.info("Removing non-valid chars.")
        df_locations.replace({u'\xa0': u' '}, regex=True, inplace=True)
        df_equipment.replace({u'\xa0': u' '}, regex=True, inplace=True)
        df_points.replace({u'\xa0': u' '}, regex=True, inplace=True)

        logger.info("File load completed.")

        # NAMESPACES
        logger.info("Generating building namespace...")
        print(self._namespaces)
        BUILDING = rdflib.Namespace(f"https://{portfolio_name}.com/{building_name}#")
        self._namespaces['building'] = BUILDING
        self.bind('building', BUILDING)
        self._namespaces['ref'] = BUILDING  # used for relative relationship references
        META = rdflib.Namespace("https://meta.com#") # temporary namespace to hold the metadata items that are used for tags. TODO: Update this.
        self._namespaces['meta'] = META
        self.bind('meta', META)
        self._building = {
            'portfolio': portfolio_name,
            'building': building_name
        }
        logger.info("Namespace generation complete.")
        print(self._namespaces)

        # PROCESS EXCEL DATA & GENERATE TRIPLES
        logger.info("Processing Building Model data...")

        # validate relationship column exists
        logger.info(f"Relationships defined by referencing column: {relationship_field}. Validating column exists on all sheets...")
        for df in [df_equipment, df_locations, df_points]:
            if not helpers.column_exists(df, relationship_field):
                logger.error(f"Model input sheet: {df.name} does not have column: {relationship_field} defined. Aborting.")
                sys.exit("Error: valid reference column not found.")
            else:
                logger.info(f"OK. {df.name} reference column found.")
        
        # generate id<>relationship_field map (entites must be related via the identifier (subject) field in the rdf graph)
        # if no custom relationship is provided this is not required.
        logger.info(f"Generating {relationship_field}<>identifier entity lookup table... ")
        df_map = pd.DataFrame(columns=['subject', 'custom'])
        for df in [df_equipment, df_locations]:
            df_temp = df[[("Brick", "identifier"), relationship_field]]
            df_temp.columns = ['subject', 'custom']
            df_map = df_map.append(df_temp) # this does not consider dups. They should be identified using the validation package.
        self._df_map = df_map
        logger.info("Successfully generated.")
        # df_map.to_csv("./_debug.csv")
        # return

        logger.info("Processing Locations...")
        triples_locations = tg.process_df(df_locations, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_locations.extend(tg.process_df(df_locations, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

        logger.info("Processing Equipment...")
        triples_equipment = tg.process_df(df_equipment, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_equipment.extend(tg.process_df(df_equipment, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))

        logger.info("Processing Points...")
        triples_points = tg.process_df(df_points, self._namespaces, "Brick", BRICK_RELATIONSHIPS, relationship_field, self._df_map)
        triples_points.extend(tg.process_df(df_points, self._namespaces, "Switch", SWITCH_RELATIONSHIPS, relationship_field, self._df_map))
        logger.info("Building model data successfully processed.")

        # ADD TRIPLES TO GRAPH
        logger.info("Adding Entities to model...")
        for triple in [*triples_locations, *triples_equipment, *triples_points]:
            self.add(triple)
        logger.info(f"{len(triples_locations)} location triples added.")
        logger.info(f"{len(triples_equipment)} equipment triples added.")
        logger.info(f"{len(triples_points)} point triples added.")

        logger.info("Generating inverse relationships...")
        self.update(sq.generate_inverse_relationships())

        # Process Extensions
        logger.info("Processing model extensions.")
        # SwitchTags
        logger.info("Processing SwitchTags")
        logger.info("Processing Equipment tags")
        tg.process_tags(self, df_equipment, self._namespaces)
        logger.info("Processing Location tags")
        tg.process_tags(self, df_locations, self._namespaces)
        logger.info("Processing Point tags")
        tg.process_tags(self, df_points, self._namespaces)

        logger.info("Entities successfully added to model.")
        logger.info("Processing complete.")

    def export(self, export_mode: str = "full", export_path: str = os.path.join(os.getcwd(), "output"), timestamp: bool = True):
        """
        Serialises a graph model to a TTL file and saves to given path
        Can generate a full, building entity only, or building equipment only model.

        :param timestamp: bool. Flag to include timestamp in filename for uniqueness
        :param export_path: dir to save ttl file. Defaults to CWD/output.
        :param export_mode: options = ["full", "building", "equipment_locations_systems"]
        :return:
        """
        # check path is OK
        if not os.path.exists(export_path):
            os.mkdir(export_path)

        # GENERATE TIMESTAMP FOR FILENAMES
        if timestamp:
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        else:
            timestamp_str = "Export"

        if export_mode == "full":
            logger.info("Exporting full brick model...")
            filename = f"{timestamp_str}_M_{self._building['portfolio']}_{self._building['building']}.ttl"
            self.serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")

        elif export_mode == "building":
            logger.info("Exporting building entities only brick model...")

            logger.info("Generating new graph...")
            g = rdflib.Graph()
            g.bind("brick", self._namespaces['brick'])
            g.bind("building", self._namespaces['building'])
            g.bind("switch", self._namespaces['switch'])

            for item in self.query(sq.query_all_triples_in_namespace(), initBindings={'namespace': self._namespaces['building']}):
                g.add(item)

            logger.info("Exporting graph...")
            filename = f"{timestamp_str}_B_{self._building['portfolio']}_{self._building['building']}.ttl"
            g.serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")

        elif export_mode == "equipment_locations_systems":
            logger.info("Exporting equipment, location, system entities brick model...")

            logger.info("Generating new graph...(this can take a few minutes)")
            g = rdflib.Graph()
            g.bind("brick", self._namespaces['brick'])
            g.bind("building", self._namespaces['building'])
            g.bind("switch", self._namespaces['switch'])
            for item in self.query(sq.query_equipment_and_location_triples_in_namespace(self._namespaces), initBindings={'namespace': self._namespaces['building']}):
                g.add(item)

            logger.info("Exporting graph...")
            filename = f"{timestamp_str}_B_{self._building['portfolio']}_{self._building['building']}_noPoints.ttl"
            g.serialize(os.path.join(export_path, filename), format='turtle')
            logger.info(f"Export complete. See file: {filename}")
        else:
            logger.info(f"Provided export mode: {export_mode} is not supported.")

    def test(self):
        print(os.getcwd())
        return os.getcwd()


