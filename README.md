# About
This package provides a series of tools to help users generate Graph models of their buildings.
This tool comes with support for Brick and Switch Extension ontologies, as well as the ability to add custom ontologies for validation and generation purposes.

The following functions are provided:
* **xlsx-validator**
Given a xlsx model in the supported template, validate the proposed model.
* **xlsx-generator**
Given an xlsx model in the supported template, generate a graph model.

Each of the tools is described in more detail below.

## Installation
Create a new environment and install from github:

 ```
 poetry add git+https://github.com/wcrd/switch-brick-tools.git@main
 ```
 In your working file simply import the package

 ```python
 import switch_brick_tools as sbt
 ```
An example working file is provide as 'example.py'.

## Dependencies
If you need to install dependencies run 
```buildoutcfg
poetry install
```
(you will need [Poetry](https://python-poetry.org/docs/) installed)

# brick-xlsx-validator

A tool that performs basic structure validation on brick models constructed in excel using the provided template [link to template].

## Validations

The tool validates the following:
* Main definition sheets exist: Equipment, Locations, Points
* Entities have an ID defined
* Entities have a valid class defined (either from Brick, or provided custom ontology)
* Relationships per entity reference other defined entities
* Checks subject uniqueness

## Example Usage

```python
import switch_brick_tools as sbt
import os

model_path = r"path_to_excel/excel_file.xlsx"

bad_rows, bad_refs, bad_classes, duplicate_ids = sbt.validate(os.path.join(model_path))
```

The validator can take a number of options:
```python
validate(filepath, load_brick: bool = True, load_switch: bool = True, brick_version: str = "1.2", switch_version: str = "1.1", custom_graph: rdflib.Graph = None, relationship_field: tuple = ("Brick", "label"))
```
`custom_graph`: a custom ontology definition in .ttl format that can be used for class validation
`relationship_field`: the field in the template which entities reference each other by. In a Brick model this would always be the 'subject' field, however some flexibility is allowed for in the spreadsheet based definition, allowing entities to reference each other by 'label' rather than a uuid, for example.

The output of the validator is:
* pandas dataframe containing the 'bad' rows from the read file, including the errors found for that row
* set of non-existing entities (i.e. bad references)
* set of invalid classes
* list of pandas dataframes containing rows with duplicated ids per sheet 




# brick-xlsx-generator
A tool that takes a template XLSX file (included) and converts it into a Brick TTL model file.



## Usage
1. Intitialize converter as an empty rdf dataset
```python
g = sbt.Dataset()
```
This will automatically load Brick and Switch ontologies. You can pass in custom versions if you need to.
```python
sbt.Dataset(load_brick: bool = True, load_switch: bool = True, brick_version: str = "1.2", switch_version: str = "1.1.4")
```

2. Process the xlsx input file to generate a populated graph model
```python
g.process(path_to_xlsx)
```
This will generate all brick relationships and process any Switch Relationships and Tags provided. See the Switch Brick Extension included to learn more.

The `process()` function can take a number of additional parameters:
```python
process(self, path_to_xlsx: str, portfolio_name: str = "example", building_name: str = "example_building", relationship_field:tuple = ("Brick", "identifier"), graph_name:str = "building")
```
`portfolio_name` & `building_name` define the URI components that the building entities will be created under. The building URI takes the form: `https://{portfolio_name}.com/{building_name}#` with a default prefix of `building`.\
`relationship_field`: the field in the template which entities reference each other by. In a Brick model this would always be the 'subject' field, however some flexibility is allowed for in the spreadsheet based definition, allowing entities to reference each other by 'label' rather than a uuid, for example.\
`graph_name`: If you needed to process multiple inputs into separate graphs, then you can provide a custom graph name per import. If you are only importing one file leave this as default.
`process_source`: Bool. If true will process source fields for BACnet and create bacnet device entities and add externalrefs to the points showing the bacnet source information.

3. Export Model to TTL
```python
g.export(export_mode="full")  # export_mode is optional
```
Export options available are:
* _full_: export everything
* _building_: export only building entities [DEFAULT]
* _equipment_locations_systems_: export only building entities and exclude points

For export "building" mode, if you have imported multiple files into separate graphs you can provide the graph name through the `graph_name` parameter to control which building graph is exported in this mode,

<!-- # Extensions
To keep things separate so that the validator and generator methods are not impact by new development, any additional information is added as part of a post processing step. Eventually this will be incorporated into the main processes; but for now much simpler to just do it here.

## Source Information - BACnet
Generate device objects and add external references to points which have BACnet sources. -->

## Note
The original `sbt.Graph()` method is still available if your legacy code uses this. It is recommended to switch to `sbt.Dataset()` as the capability is greatly improved.

## References
Parts of this code are based on code provided by the py-brickschema package.