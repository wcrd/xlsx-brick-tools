import switch_brick_tools as sbt
import os


document_filepath = os.path.join("./testing_files/model_v1.xlsx")

## Validation
bad_rows, bad_refs, bad_classes, duplicate_ids = sbt.validate(document_filepath, switch_version="1.1.7", reference_field=("Brick", "label"))

## Generation
x = sbt.Dataset()

# rdf model namespacing
PORTFOLIO = "example_portfolio"
BUILDING_NAME = "example_building"


x.process(document_filepath, relationship_field=("Brick", "label"), portfolio_name=PORTFOLIO, building_name=BUILDING_NAME)

# export graph model to TTL file
g.export()                          # export full model
g.export(export_mode="building")    # export buidling entities only