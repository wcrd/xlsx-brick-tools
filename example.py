import brick_xlsx_validator as bv
import os

# model_path = r"C:\Users\WillDavidson\Desktop\DB-BrickModelInput-V6.xlsx"
# model_path = r"C:\Users\WillDavidson\OneDrive - Switch Automation\R&D\Brick Modelling\sites\Carrier CIB\20210728 - Carrier-CiB_subParted _newFormat.xlsx"
# model_path = r"C:\Users\WillDavidson\OneDrive - Switch Automation\R&D\Brick Modelling\sites\Carrier CIB\20210728 - Carrier-CiB _newFormat.xlsx"
# model_path = r"C:\Users\WillDavidson\Downloads\eau_clair_equip.xlsx"
model_path = r"C:\Users\WillDavidson\OneDrive - Switch Automation\R&D\Brick Modelling\sites\Carrier Lerner\20210808 - Carrier-Lerner _newFormat.xlsx"

df, bad_refs, bad_classes, duplicate_ids = bv.validate(os.path.join(model_path), switch_version="1.1.7", reference_field=("Brick", "label"))



import brick_xlsx_generator as bg
import os

# xlsx path
document_filepath = r"PATH TO FILE"

# rdf model namespacing
PORTFOLIO = "example_portfolio"
BUILDING_NAME = "example_building"

# initialise converter
g = bg.Graph()
# process input file to generate graph model
g.process(document_filepath, PORTFOLIO, BUILDING_NAME)
# export graph model to TTL file
g.export()                          # export full model
g.export(export_mode="building")    # export buidling entities only